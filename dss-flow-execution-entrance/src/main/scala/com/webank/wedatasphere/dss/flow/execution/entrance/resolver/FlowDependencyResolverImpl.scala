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

package com.webank.wedatasphere.dss.flow.execution.entrance.resolver

import java.util

import com.webank.wedatasphere.dss.flow.execution.entrance.{FlowContext}
import com.webank.wedatasphere.dss.flow.execution.entrance.job.FlowEntranceJob

import scala.collection.JavaConversions._
import com.webank.wedatasphere.linkis.common.utils.Logging
import org.springframework.stereotype.Component


/**
  * created by chaogefeng on 2019/11/5 10:24
  * Description:
  */
@Component
class FlowDependencyResolverImpl extends FlowDependencyResolver with Logging {
  override def resolvedFlow(flowJob: FlowEntranceJob) = {

    info(s"${flowJob.getId} Start to get executable node")

    val flowContext: FlowContext = flowJob.getFlowContext
    val nodes  = flowContext.getPendingNodes.toMap.values.map(_.getNode)
    //父类所有节点都已经完成的判断,循环遍历所有的父类,并且他们的状态是skip 或则 成功,或者失败
    def  isAllParentDependencyCompleted(parents:util.List[String]): Boolean = {
      for (parent <- parents){
        if( ! flowContext.isNodeCompleted(parent)) return false
      }
      true
    }
    //目前为止,nodes只是一个顺序集合,其顺序也不能代表任何工作了的依赖信息
    nodes.foreach{ node =>
      val nodeName = node.getName
      //判断当前节点是否可以执行
      //1. PendingNodes 中有这个节点(工作流中的节点名不能重复(这是前台保存时做的修改和判断))
      //2.节点并非处于完成状态
      //3. 它的父节点都执行完成了
      def isCanExecutable: Boolean = {
        (flowContext.getPendingNodes.containsKey(nodeName)
          && !FlowContext.isNodeRunning(nodeName, flowContext)
          && !flowContext.isNodeCompleted(nodeName)
          && isAllParentDependencyCompleted(node.getDependencys))
      }
      if (isCanExecutable) flowContext synchronized {
        //将满足当前节点的NodeRunnaer状态init 转换为scheduler,一般来说,第一次执行只有并发的第一个节点是被进行转化的
        if (isCanExecutable) flowContext.getPendingNodes.get(nodeName).tunToScheduled()
      }
    }
    info(s"${flowJob.getId} Finished to get executable node(${flowContext.getScheduledNodes.size()})")

  }


}