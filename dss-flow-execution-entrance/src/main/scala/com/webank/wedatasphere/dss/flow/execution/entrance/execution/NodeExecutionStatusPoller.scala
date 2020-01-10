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

package com.webank.wedatasphere.dss.flow.execution.entrance.execution


import java.util.concurrent.LinkedBlockingQueue

import com.webank.wedatasphere.dss.flow.execution.entrance.node.NodeRunner
import com.webank.wedatasphere.linkis.common.utils.Logging

/**
  * Created by peacewong on 2019/11/17.
  */
class NodeExecutionStatusPoller(nodeRunnerQueue: LinkedBlockingQueue[NodeRunner] ) extends Runnable with Logging{



  override def run(): Unit = {

    val runner = nodeRunnerQueue.take()
    //定时器会主动取出来,如果已经完成,就取出,如果没完成就继续放回去
    if(! runner.isLinkisJobCompleted) {
      nodeRunnerQueue.put(runner)
    } else {
      info(s"node ${runner.getNode.getName} status is completed")
    }
  }

}
