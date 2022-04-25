/*
 * @Author: gitsrc、Joel Wu
 * @Date: 2020-07-09 11:50:27
 * @LastEditors: gitsrc
 * @LastEditTime: 2020-07-09 14:06:59
 * @FilePath: /ServiceCar/utils/rediscluster/batch.go
 */

package rediscluster

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Batch pack multiple commands, which should be supported by Do method.
type Batch struct {
	cluster        *Cluster
	batches        []nodeBatch
	index          []int
	isSlaveOperate bool
	numSeed        uint64 //数量种子：可以用在随机上，避免性能开销
}

type nodeBatch struct {
	node       *redisNode
	masterNode *redisNode //master node :仅仅在node 为slave node是进行填充，以备slave节点请求降级
	cmds       []nodeCommand

	err  error
	done chan int
}

type nodeCommand struct {
	cmd   string
	args  []interface{}
	reply interface{}
	err   error
}

// NewBatch create a new batch to pack mutiple commands.
func (cluster *Cluster) NewBatch(numSeed uint64) *Batch {
	return &Batch{
		cluster:        cluster,
		batches:        make([]nodeBatch, 0),
		index:          make([]int, 0),
		isSlaveOperate: false,
		numSeed:        numSeed,
	}
}

func (batch *Batch) SetIsSlaveOperate() {
	if batch.cluster.IsSlaveOperate() {
		batch.isSlaveOperate = true
	} else {
		batch.isSlaveOperate = false
	}
}

func (batch *Batch) GetIsSlaveOperate() bool {
	return batch.isSlaveOperate
}

// Put add a redis command to batch, DO NOT put MGET/MSET/MSETNX.
func (batch *Batch) Put(isSlaveOperate bool, cmd string, args ...interface{}) error {
	if len(args) < 1 {
		return fmt.Errorf("Put: no key found in args")
	}

	if cmd == "MGET" || cmd == "MSET" || cmd == "MSETNX" {
		return fmt.Errorf("Put: %s not supported", cmd)
	}

	var node *redisNode
	var masterNode *redisNode
	var err error

	switch strings.ToUpper(cmd) {
	case "GET":
		//如果开启了读写分离，并且当前概率正好可以进行slave操作: 选择一个可用的slave节点
		//如果是slave节点操作
		if isSlaveOperate {
			node, masterNode, err = batch.cluster.getAnSlaveNodeByNumSeed(args[0], batch.numSeed) //获取从节点、并获取从对应的master节点
			//如果从节点没有可用的，例如从节点数量为0
			if err != nil {
				//选择slave节点出错，发送cluster更新信号(根据master节点)
				//KafkaLoger.CErrorf("RedisCluster Batch-Put-GET-SLAVE wrong. [%s] -> %s.", args[0], err)

				//随机选择一个master节点，并进行cluster slot集群信息的更新过程。
				go batch.cluster.UpdateSlotsByRandomMasterNode()

				//选择cluser中的master节点进行key的服务承载
				node, err = batch.cluster.getNodeByKey(args[0])
			}
		} else {
			//当前情况 slave模式关闭，或者当前概率没有覆盖到slave节点 ： 选择master节点
			node, err = batch.cluster.getNodeByKey(args[0])
		}
	default:
		node, err = batch.cluster.getNodeByKey(args[0])
	}
	//log.Println(batch.cluster.IsSlaveOperate())
	//slaveNode,err := batch.cluster.getAnRandomSlaveNodeByKey(args[0])
	//
	if err != nil {
		return fmt.Errorf("Put: %v", err)
	}

	var i int
	//batch 数量预计算
	batchesLen := len(batch.batches)

	for i = 0; i < batchesLen; i++ {
		if batch.batches[i].node == node {
			batch.batches[i].cmds = append(batch.batches[i].cmds,
				nodeCommand{cmd: cmd, args: args})

			batch.index = append(batch.index, i)
			break
		}
	}

	//如果没有找到这个batch
	if i == batchesLen {
		batch.batches = append(batch.batches,
			nodeBatch{
				node:       node,
				masterNode: masterNode, //此处masterNode可能为nil，只要在node为slave节点的情况下
				cmds:       []nodeCommand{{cmd: cmd, args: args}},
				done:       make(chan int)})
		batch.index = append(batch.index, i)
	}

	return nil
}

// RunBatch execute commands in batch simutaneously. If multiple commands are
// directed to the same node, they will be merged and sent at once using pipeling.
func (cluster *Cluster) RunBatch(bat *Batch) ([]interface{}, error) {
	batchesCount := len(bat.batches)

	var batchesWg sync.WaitGroup
	batchesWg.Add(batchesCount)

	for i := range bat.batches {
		go doBatch(cluster, &bat.batches[i], &batchesWg, bat.isSlaveOperate)
	}

	batchesWg.Wait()

	var replies []interface{}

	//bat.index存储的是m指令中每个key所对应的pipeline-id，通过这个id进行响应索引
	for _, i := range bat.index {
		//如果redis集群reply出现 集群宕机,则返回错误
		if checkReply(bat.batches[i].cmds[0].reply) == kRespClusterDown {

			//KafkaLoger.CErrorf("RedisCluster Runbatch wrong: ClusterDown.")
			bat.batches[i].cmds[0].reply = nil //由于这部分错误，所以返回nil，不耽误其他数据

			cluster.UpdateSlotsInfoByRandomNode(bat.batches[i].node) //更新集群信息

			return nil, Cluster_Down_Error

		}

		//出现非宕机级别的错误：可能是主节点损坏，网络出错，则数据设置为nil，更新节点
		if bat.batches[i].err != nil {
			//如果集群信息更新失败，则上报错误，因为这个错误属于最严重级别，节点信息无法更新，所有工作无法继续
			//if err != nil {
			//	go KafkaLoger.Errorf("RedisCluster Runbatch wrong. err: %s", err.Error())
			//	return nil, bat.batches[i].err
			//}

			//这一步 代表更新节点没出错，把数据设置为nil，不耽误其他数据: 这种情况是集群还没宕机，正在主从切换过程
			bat.batches[i].cmds[0].reply = nil
			//KafkaLoger.CErrorf("RedisCluster Run batch ID(%d) wrong. err: %s", i, bat.batches[i].err)

			cluster.UpdateSlotsInfoByRandomNode(bat.batches[i].node)
		}

		replies = append(replies, bat.batches[i].cmds[0].reply)

		//因为消费了bat.batches[i].cmds的第一个元素，所以把slice向后推一个，便于下次循环使用bat.batches[i].cmds
		bat.batches[i].cmds = bat.batches[i].cmds[1:]
	}

	return replies, nil
}

//根据cluster中的nodes进行同步模式节点更新
//func (cluster *Cluster) UpdateSlotsInfoByAllNode() (err error) {
//	//cluster.rwLock.RLock()
//	for _, node := range cluster.nodes {
//		err = cluster.Update(node)
//
//		//如果节点更新失败 则 寻找下一个节点
//		if err != nil {
//			continue
//		}
//
//		break
//	}
//	//cluster.rwLock.RUnlock()
//	return
//}

func doBatch(cluster *Cluster, batch *nodeBatch, wg *sync.WaitGroup, isSlaveOperate bool) {
	defer wg.Done()

	node := batch.node //获取batch里面的 node ： 可能为master 或者slave

	conn, err := node.getConn() //这里面有conn dial操作 ： 如果超时则报错
	if err != nil {
		//如果slave节点出现问题 则发送集群更新信号
		go cluster.UpdateSlotsByRandomMasterNode()

		//如果node节点类型为slave node 并且 batch中masterNode 不为nil
		if node.NodeType == SLAVE_NODE && batch.masterNode != nil {

			node = batch.masterNode //slave降级为master节点
			conn, err = node.getConn()

			if err != nil {
				batch.err = err
				return
			}

			//执行到此处，代表成功降级
			//KafkaLoger.CErrorf("Slave Node doBatch getConn wrong. Downgrade : %s -> %s", batch.node.address, node.address)

		} else {

			batch.err = err
			return
		}
	}

	////写入batch 中的cmds，性能损耗点 ： 通过内存承载来进行优化
	//for i := range batch.cmds {
	//	conn.sendM(batch.cmds[i].cmd, batch.cmds[i].args...)
	//}

	//刷新内存缓冲区，让bufio读写指针位置重置
	//conn.flushM()

	//向redis集群中的节点写入pipeline指令
	//if conn.writeTimeout > 0 {
	//	conn.c.SetWriteDeadline(time.Now().Add(conn.writeTimeout))
	//}
	//_, err = conn.c.Write(conn.writerMemory.Bytes())
	//
	//if err != nil {
	//	batch.err = err
	//	conn.shutdown()
	//	return
	//}

	//如果从节点开启读写分离 则发送readonly命令
	if node.NodeType == SLAVE_NODE {
		err = conn.send("READONLY")

		if err != nil {
			batch.err = err
			conn.shutdown()
			return
		}
		//conn.flush()
	}

	for i := range batch.cmds {
		err = conn.send(batch.cmds[i].cmd, batch.cmds[i].args...)

		if err != nil {
			batch.err = err
			conn.shutdown()
			return
		}
	}

	//刷新conn缓冲区
	err = conn.flush()
	if err != nil {
		batch.err = err
		conn.shutdown()
		return
	}

	//如果是slave read 首先进行READONLY指令确认
	if node.NodeType == SLAVE_NODE {
		reply, err := conn.receive()

		if err != nil {
			batch.err = err
			conn.shutdown()
			return
		}

		if checkReply(reply) != kRespOK {
			batch.err = errors.New("doBatch : [READONLY command] -> Reply is not OK.")
			conn.shutdown()
			return
		}
	}

	//读取数据 修改版：增加move ask等指令的支持和节点更新感应
	for i := range batch.cmds {
		reply, err := conn.receive()
		if err != nil {
			batch.err = err
			conn.shutdown()
			return
		}

		//检查reply类型 : 根据不同的类型进行相关操作：MOVE\ASK\TIMEOUT
		resp := checkReply(reply)

		switch resp {
		case kRespOK, kRespError:
			err = nil
		case kRespMove:
			//此处在高并发+slots循环多次集中迁移时，会出现数据的多级别MOVE，对于多级别MOVE 要进行到底，一般频率为20万次中出现10次
			for {
				reply, err = cluster.handleMove(node, reply.(redisError).Error(), batch.cmds[i].cmd, batch.cmds[i].args)
				respType := checkReply(reply)

				//如果成功迁移了：resptype就不会为MOVE，然后针对reply进行类型判断，并把此次结果传到外部，跳出循环
				if respType != kRespMove {
					switch respType {
					case kRespOK, kRespError:
						err = nil
					case kRespAsk:
						reply, err = cluster.handleAsk(node, reply.(redisError).Error(), batch.cmds[i].cmd, batch.cmds[i].args)
					case kRespConnTimeout:
						reply, err = cluster.handleConnTimeout(node, batch.cmds[i].cmd, batch.cmds[i].args)
					case kRespClusterDown:
						err = Cluster_Down_Error
					}

					//此处的break用来跳出 211行 循环
					break
				}
			}
		case kRespAsk:
			reply, err = cluster.handleAsk(node, reply.(redisError).Error(), batch.cmds[i].cmd, batch.cmds[i].args)
		case kRespConnTimeout:
			reply, err = cluster.handleConnTimeout(node, batch.cmds[i].cmd, batch.cmds[i].args)
		case kRespClusterDown:
			err = Cluster_Down_Error
		}

		batch.cmds[i].reply, batch.cmds[i].err = reply, err
	}

	node.releaseConn(conn)
}

//优化2：新版redis协议流读取
/*conn.readerParser.Reset() //连接所绑定的reader缓冲区 重置读写指针
for i := range batch.cmds {
	conn.pending -= 1
	command, err := conn.readerParser.ReadCommand()
	if err != nil {
		log.Println(err)
		break
	}
	batch.cmds[i].reply, batch.cmds[i].err = command.GetSingleArgv(), err
}
*/
