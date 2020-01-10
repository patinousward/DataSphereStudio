/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.webank.wedatasphere.dss.flow.execution.entrance.node

import java.util

import com.webank.wedatasphere.dss.appjoint.scheduler.entity.SchedulerNode
import com.webank.wedatasphere.dss.flow.execution.entrance.conf.FlowExecutionEntranceConfiguration
import com.webank.wedatasphere.dss.linkis.node.execution.job.{JobTypeEnum, LinkisJob}
import com.webank.wedatasphere.dss.flow.execution.entrance.listener.NodeRunnerListener
import com.webank.wedatasphere.dss.flow.execution.entrance.log.FlowExecutionLog
import com.webank.wedatasphere.dss.flow.execution.entrance.node.NodeExecutionState.NodeExecutionState
import com.webank.wedatasphere.dss.linkis.node.execution.execution.impl.LinkisNodeExecutionImpl
import com.webank.wedatasphere.dss.linkis.node.execution.listener.LinkisExecutionListener
import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}

/**
  * Created by peacewong on 2019/11/5.
  */
class DefaultNodeRunner extends NodeRunner with Logging {

  private var node: SchedulerNode = _

  private var linkisJob: LinkisJob = _

  private var canceled: Boolean = false

  private var status: NodeExecutionState = NodeExecutionState.Inited

  private var nodeRunnerListener: NodeRunnerListener = _

  private var executedInfo: String = _

  private var startTime: Long = _

  override def getNode: SchedulerNode = this.node

  def setNode(schedulerNode: SchedulerNode): Unit = {
    this.node = schedulerNode
  }

  override def getLinkisJob: LinkisJob = this.linkisJob


  override def isCanceled: Boolean = this.canceled

  override def getStatus: NodeExecutionState = {
    this.status
  }

  override def isLinkisJobCompleted: Boolean = Utils.tryCatch{
    //当linkisjob执行成功后(或者失败后),NodeRunner这边的状态肯定还是running的,这里走false
    if(NodeExecutionState.isCompleted(getStatus)) return true
    //这个方法中进行翻转成功,亦或者job在engine那边执行失败
    //这里查询linkisjob的状态,并且未完成的话,就会去查询并且翻转荒唐
    val toState = NodeExecutionState.withName(LinkisNodeExecutionImpl.getLinkisNodeExecution.getState(this.linkisJob))
    if (NodeExecutionState.isCompleted(toState)) {
      val listener = LinkisNodeExecutionImpl.getLinkisNodeExecution.asInstanceOf[LinkisExecutionListener]
      listener.onStatusChanged(getStatus.toString, toState.toString, this.linkisJob)
      this.transitionState(toState)
      info(s"Finished to execute node of ${this.node.getName}")
      true
    } else {
      false
    }
  }{ t =>
    warn(s"Failed to get ${this.node.getName} linkis job states", t)
    false
  }

  override def setNodeRunnerListener(nodeRunnerListener: NodeRunnerListener): Unit = this.nodeRunnerListener = nodeRunnerListener

  override def getNodeRunnerListener: NodeRunnerListener = this.nodeRunnerListener

  override def run(): Unit = {
    info(s"start to run node of ${node.getName}")
    try {
      val jobProps = node.getDWSNode.getParams.remove(FlowExecutionEntranceConfiguration.PROPS_MAP) match {
        case propsMap: util.Map[String, String] => propsMap
        case _ => new util.HashMap[String, String]()
      }
      //创建一个linkisJob对象,给this.linkisJob赋值
      this.linkisJob = AppJointJobBuilder.builder().setNode(node).setJobProps(jobProps).build.asInstanceOf[LinkisJob]
      this.linkisJob.setLogObj(new FlowExecutionLog(this))
      //set start time
      this.setStartTime(System.currentTimeMillis())
      if (JobTypeEnum.EmptyJob == this.linkisJob.getJobType) {
        //空节点直接不执行,返回成功
        warn("This node is empty type")
        this.transitionState(NodeExecutionState.Succeed)
        return
      }
      //正真提交执行的地方,其实只是使用ujesclient去http请求linkis,这里应该是阻塞的,但是Entranc的execute接口,提交scheduler后
      //就会返回id了,勉强算异步
      LinkisNodeExecutionImpl.getLinkisNodeExecution.runJob(this.linkisJob)
      info(s"Finished to run node of ${node.getName}")
      /*LinkisNodeExecutionImpl.getLinkisNodeExecution.waitForComplete(this.linkisJob)
      val listener = LinkisNodeExecutionImpl.getLinkisNodeExecution.asInstanceOf[LinkisExecutionListener]
      val toState = LinkisNodeExecutionImpl.getLinkisNodeExecution.getState(this.linkisJob)
      listener.onStatusChanged(getStatus.toString, toState, this.linkisJob)
      this.transitionState(NodeExecutionState.withName(toState))
      info(s"Finished to execute node of ${node.getName}")*/
    } catch {
          //这里的异常翻转失败只是提交job到scheduler执行失败,如果是job本身在engine中执行失败呢?
          //答案在isLinkisJobCompleted 方法中,这个方法被定时器不断调用
      case t: Throwable =>
        warn(s"Failed to execute node of ${node.getName}", t)
        this.transitionState(NodeExecutionState.Failed)

    }

  }


  override def cancel(): Unit = if (!this.canceled) this synchronized {
    if (this.canceled) return
    this.canceled = true
    if (this.linkisJob != null && this.linkisJob.getJobExecuteResult != null)
      LinkisNodeExecutionImpl.getLinkisNodeExecution.cancel(this.linkisJob)
    this.transitionState(NodeExecutionState.Cancelled)
    warn(s"This node(${node.getName}) has been canceled")
  }

  override def pause(): Unit = {}

  override def resume(): Boolean = true

  override def setStatus(nodeExecutionState: NodeExecutionState): Unit = this.status = nodeExecutionState

  override def getNodeExecutedInfo(): String = this.executedInfo

  override def setNodeExecutedInfo(info: String): Unit = this.executedInfo = info

  override def getStartTime(): Long = this.startTime

  override def setStartTime(startTime: Long): Unit = this.startTime = startTime

}
