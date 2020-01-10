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

package com.webank.wedatasphere.dss.flow.execution.entrance.job.parser

import com.webank.wedatasphere.dss.appjoint.scheduler.entity.{AbstractSchedulerProject, SchedulerFlow}
import com.webank.wedatasphere.dss.common.entity.project.DWSProject
import com.webank.wedatasphere.dss.common.protocol.RequestDWSProject
import com.webank.wedatasphere.dss.flow.execution.entrance.conf.FlowExecutionEntranceConfiguration
import com.webank.wedatasphere.dss.flow.execution.entrance.entity.FlowExecutionCode
import com.webank.wedatasphere.dss.flow.execution.entrance.job.FlowEntranceJob
import com.webank.wedatasphere.dss.flow.execution.entrance.parser.FlowExecutionProjectParser
import com.webank.wedatasphere.dss.flow.execution.entrance.tuning.FlowExecutionProjectTuning
import com.webank.wedatasphere.dss.linkis.node.execution.utils.LinkisJobExecutionUtils
import com.webank.wedatasphere.linkis.common.utils.{Logging, Utils}
import com.webank.wedatasphere.linkis.rpc.Sender
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

import scala.collection.JavaConversions._


@Order(1) //先解析
@Component
class FlowJobFlowParser extends FlowEntranceJobParser with Logging {


  @Autowired
  var flowExecutionProjectParser: FlowExecutionProjectParser = _

  @Autowired
  var flowExecutionProjectTuning: FlowExecutionProjectTuning = _


  override def parse(flowEntranceJob: FlowEntranceJob): Unit = {
    info(s"${flowEntranceJob.getId} start to parse flow")
    val code = flowEntranceJob.jobToExecuteRequest().code
    //将前台的请求参数 反序列化
    val flowExecutionCode = LinkisJobExecutionUtils.gson.fromJson(code, classOf[FlowExecutionCode])
    //rpc请求,获取当前flow,以及它的所有父flow
    getDWSProjectByCode(flowExecutionCode) match {
      case dwsProject: DWSProject =>
        //parse和tunning后,将dwsProject转为schedulerproject
        val project = this.flowExecutionProjectParser.parseProject(dwsProject)
        this.flowExecutionProjectTuning.tuningSchedulerProject(project)

        //获取所有的flowallFlows
        val allFlows = project.asInstanceOf[AbstractSchedulerProject].getProjectVersions.head.getFlows

        var dwsFlow: SchedulerFlow = null //根据id,获取到当前执行的flow
        for (flow <- allFlows) {
          if (flowExecutionCode.getFlowId == flow.getId) {
            dwsFlow = flow
          }
        }
        //先保存dwsProject对象
        flowEntranceJob.setDwsProject(dwsProject)
        //set flow
        flowEntranceJob.setFlow(dwsFlow)
    }
    info(s"${flowEntranceJob.getId} finished to parse flow")
  }

  private def getDWSProjectByCode(flowExecutionCode: FlowExecutionCode) = {
    val req = new RequestDWSProject(flowExecutionCode.getFlowId, flowExecutionCode.getVersion, flowExecutionCode.getProjectVersionId)
    Sender.getSender(FlowExecutionEntranceConfiguration.SCHEDULER_APPLICATION.getValue).ask(req)
  }

}
