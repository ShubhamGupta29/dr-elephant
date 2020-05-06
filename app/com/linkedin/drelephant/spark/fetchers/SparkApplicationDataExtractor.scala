/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.fetchers

import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.fetchers.statusapiv1.{ApplicationAttemptInfoImpl, ApplicationInfoImpl, ExecutorSummaryImpl, JobDataImpl, StageDataImpl}
import org.apache.spark.status.api.v1.{ApplicationEnvironmentInfo, ApplicationInfo, ExecutorSummary, JobData, StageData, StageStatus}

import scala.collection.mutable.HashMap

object SparkApplicationDataExtractor {
  def extractSparkApplicationDataFromAppStatusStore(appInfo: ApplicationInfo, environmentInfo:
  ApplicationEnvironmentInfo, jobsList: Seq[JobData], stageList: Seq[StageData], executorList:
  Seq[ExecutorSummary], executorIdToMaxMemoryUsed: HashMap[String, Long]): SparkApplicationData = {
    new SparkApplicationData(
      appInfo.id,
      extractAppConfigurations(environmentInfo),
      extractApplicationInfo(appInfo),
      extractJobDataList(jobsList),
      extractStageDataList(stageList),
      extractExecutorSummaries(executorList, executorIdToMaxMemoryUsed))
  }

  private def extractAppConfigurations(environmentInfo: ApplicationEnvironmentInfo): Map[String, String] = {
    environmentInfo.sparkProperties.toMap
  }

  private def extractApplicationInfo(appInfo: ApplicationInfo): statusapiv1.ApplicationInfo = {
    new ApplicationInfoImpl(appInfo.id, appInfo.name, appInfo.attempts.map(attempt =>
      new ApplicationAttemptInfoImpl(
        attempt.attemptId,
        attempt.startTime,
        attempt.endTime,
        attempt.sparkUser,
        attempt.completed
      )))
  }

  private def extractJobDataList(jobDataSeq: Seq[JobData]): Seq[JobDataImpl] = {
    jobDataSeq.map(jobInfo =>
      new JobDataImpl(
        jobInfo.jobId,
        jobInfo.jobId.toString,
        jobInfo.description,
        jobInfo.submissionTime,
        jobInfo.completionTime,
        jobInfo.stageIds,
        jobInfo.jobGroup,
        jobInfo.status,
        jobInfo.numTasks,
        jobInfo.numActiveTasks,
        jobInfo.numCompletedTasks,
        jobInfo.numSkippedTasks,
        jobInfo.numFailedTasks,
        jobInfo.numActiveStages,
        jobInfo.numCompletedStages,
        jobInfo.numSkippedStages,
        jobInfo.numFailedStages
      )
    )
  }

  private def extractStageDataList(stageDataSeq: Seq[StageData]): Seq[StageDataImpl] = {
    stageDataSeq.map(stageInfo =>
      new StageDataImpl(
        getStageStatus(stageInfo.status),
        stageInfo.stageId,
        stageInfo.attemptId,
        stageInfo.numActiveTasks,
        stageInfo.numCompleteTasks,
        stageInfo.numFailedTasks,
        stageInfo.executorRunTime,
        stageInfo.inputBytes,
        inputRecords = 0,
        stageInfo.outputBytes,
        outputRecords = 0,
        stageInfo.shuffleReadBytes,
        shuffleReadRecords = 0,
        stageInfo.shuffleWriteBytes,
        shuffleWriteRecords = 0,
        stageInfo.memoryBytesSpilled,
        stageInfo.diskBytesSpilled,
        stageInfo.name,
        stageInfo.details,
        stageInfo.schedulingPool,
        accumulatorUpdates = Seq.empty,
        tasks = None,
        executorSummary = None
      ))
  }

  private def extractExecutorSummaries(executorSummarySeq: Seq[ExecutorSummary], executorIdToMaxMemoryUsed:
  HashMap[String, Long]): Seq[ExecutorSummaryImpl] = {
    executorSummarySeq.map(executorSummary =>
      new ExecutorSummaryImpl(
        executorSummary.id,
        executorSummary.hostPort,
        executorSummary.rddBlocks,
        executorIdToMaxMemoryUsed.getOrElse(executorSummary.id, 0L),
        executorSummary.diskUsed,
        executorSummary.activeTasks,
        executorSummary.failedTasks,
        executorSummary.completedTasks,
        executorSummary.totalTasks,
        executorSummary.totalDuration,
        executorSummary.totalInputBytes,
        executorSummary.totalShuffleRead,
        executorSummary.totalShuffleWrite,
        executorSummary.maxMemory,
        executorSummary.totalGCTime,
        executorLogs = Map.empty
      )
    )
  }

  private def getStageStatus(status: StageStatus): statusapiv1.StageStatus = {
    statusapiv1.StageStatus.fromString(status.name())
  }
}
