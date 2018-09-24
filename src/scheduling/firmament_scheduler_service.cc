/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#include <grpc++/grpc++.h>

#include <ctime>
#include "base/resource_status.h"
#include "base/resource_topology_node_desc.pb.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/trace_generator.h"
#include "misc/utils.h"
#include "misc/wall_time.h"
#include "platforms/sim/simulated_messaging_adapter.h"
#include "scheduling/event_driven_scheduler.h"
#include "scheduling/firmament_scheduler.grpc.pb.h"
#include "scheduling/firmament_scheduler.pb.h"
#include "scheduling/flow/flow_scheduler.h"
#include "scheduling/knowledge_base_populator.h"
#include "scheduling/scheduler_interface.h"
#include "scheduling/scheduling_delta.pb.h"
#include "scheduling/simple/simple_scheduler.h"
#include "storage/simple_object_store.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using firmament::scheduler::FlowScheduler;
using firmament::scheduler::ObjectStoreInterface;
using firmament::scheduler::SchedulerInterface;
using firmament::scheduler::SchedulerStats;
using firmament::scheduler::SimpleScheduler;
using firmament::scheduler::TopologyManager;
using firmament::platform::sim::SimulatedMessagingAdapter;

DEFINE_string(firmament_scheduler_service_address, "127.0.0.1",
              "The address of the scheduler service");
DEFINE_string(firmament_scheduler_service_port, "9090",
              "The port of the scheduler service");
DECLARE_bool(resource_stats_update_based_on_resource_reservation);
DEFINE_string(service_scheduler, "flow", "Scheduler to use: flow | simple");
DEFINE_uint64(queue_based_scheduling_time, 100, "Queue Based Schedule run time");

namespace firmament {

class FirmamentSchedulerServiceImpl final : public FirmamentScheduler::Service {
 public:
  FirmamentSchedulerServiceImpl() {
    job_map_.reset(new JobMap_t);
    task_map_.reset(new TaskMap_t);
    resource_map_.reset(new ResourceMap_t);
    knowledge_base_.reset(new KnowledgeBase);
    topology_manager_.reset(new TopologyManager);
    ResourceStatus* top_level_res_status = CreateTopLevelResource();
    top_level_res_id_ =
        ResourceIDFromString(top_level_res_status->descriptor().uuid());
    sim_messaging_adapter_ = new SimulatedMessagingAdapter<BaseMessage>();
    trace_generator_ = new TraceGenerator(&wall_time_);
    if (FLAGS_service_scheduler == "flow") {
      scheduler_ = new FlowScheduler(
          job_map_, resource_map_,
          top_level_res_status->mutable_topology_node(), obj_store_, task_map_,
          knowledge_base_, topology_manager_, sim_messaging_adapter_, NULL,
          top_level_res_id_, "", &wall_time_, trace_generator_, &labels_map_,
          &affinity_antiaffinity_tasks_);
    } else if (FLAGS_service_scheduler == "simple") {
      scheduler_ = new SimpleScheduler(
          job_map_, resource_map_,
          top_level_res_status->mutable_topology_node(), obj_store_, task_map_,
          knowledge_base_, topology_manager_, sim_messaging_adapter_, NULL,
          top_level_res_id_, "", &wall_time_, trace_generator_);
    } else {
      LOG(FATAL) << "Flag specifies unknown scheduler "
                 << FLAGS_service_scheduler;
    }

    kb_populator_ = new KnowledgeBasePopulator(knowledge_base_);
  }

  ~FirmamentSchedulerServiceImpl() {
    delete scheduler_;
    delete sim_messaging_adapter_;
    delete trace_generator_;
    delete kb_populator_;
  }

  void HandlePlacementDelta(const SchedulingDelta& delta) {
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, delta.task_id());
    CHECK_NOTNULL(td_ptr);
    td_ptr->set_start_time(wall_time_.GetCurrentTimestamp());
  }

  void HandlePreemptionDelta(const SchedulingDelta& delta) {
    // TODO(ionel): Implement!
  }

  void HandleMigrationDelta(const SchedulingDelta& delta) {
    // TODO(ionel): Implement!
  }

  // Helper function that update the knowledge base with resource stats samples
  // based on task resource request reservation. We can use this function when
  // we do not have external dynamic resource stats provider like heapster in
  // kubernetes. If add is true, then tast resource request is subtracted from
  // available machine resources. else tast resource request is added back to
  // available machine resources.
  void UpdateMachineSamplesToKnowledgeBaseStatically(
      const TaskDescriptor* td_ptr, bool add) {
    ResourceID_t res_id = ResourceIDFromString(td_ptr->scheduled_to_resource());
    ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
    ResourceStats resource_stats;
    CpuStats* cpu_stats = resource_stats.add_cpus_stats();
    bool have_sample = knowledge_base_->GetLatestStatsForMachine(
        ResourceIDFromString(rs->mutable_topology_node()->parent_id()),
        &resource_stats);
    if (have_sample) {
      if (add) {
        cpu_stats->set_cpu_allocatable(cpu_stats->cpu_allocatable() +
                                       td_ptr->resource_request().cpu_cores());
        resource_stats.set_mem_allocatable(
            resource_stats.mem_allocatable() +
            td_ptr->resource_request().ram_cap());
      } else {
        cpu_stats->set_cpu_allocatable(cpu_stats->cpu_allocatable() -
                                       td_ptr->resource_request().cpu_cores());
        resource_stats.set_mem_allocatable(
            resource_stats.mem_allocatable() -
            td_ptr->resource_request().ram_cap());
      }
      double cpu_utilization =
          (cpu_stats->cpu_capacity() - cpu_stats->cpu_allocatable()) /
          (double)cpu_stats->cpu_capacity();
      cpu_stats->set_cpu_utilization(cpu_utilization);
      double mem_utilization =
          (resource_stats.mem_capacity() - resource_stats.mem_allocatable()) /
          (double)resource_stats.mem_capacity();
      resource_stats.set_mem_utilization(mem_utilization);
      knowledge_base_->AddMachineSample(resource_stats);
    }
  }

  Status Schedule(ServerContext* context, const ScheduleRequest* request,
                  SchedulingDeltas* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(
        scheduler_->scheduling_lock_);
    SchedulerStats sstat;
    vector<SchedulingDelta> deltas;
    scheduler_->ScheduleAllJobs(&sstat, &deltas);
    clock_t start = clock();
    uint64_t elapsed = 0;
    // Schedule tasks having pod affinity/anti-affinity
    while (affinity_antiaffinity_tasks_.size() &&
           (elapsed < FLAGS_queue_based_scheduling_time)) {
      scheduler_->ScheduleAllQueueJobs(&sstat, &deltas);
      clock_t stop = clock();
      elapsed = (double)(stop - start) * 1000.0 / CLOCKS_PER_SEC;
    }
    // Extract results
    if (deltas.size()) {
      LOG(INFO) << "Got " << deltas.size() << " scheduling deltas";
    }
    for (auto& d : deltas) {
      // LOG(INFO) << "Delta: " << d.DebugString();
      SchedulingDelta* ret_delta = reply->add_deltas();
      ret_delta->CopyFrom(d);
      if (d.type() == SchedulingDelta::PLACE) {
        HandlePlacementDelta(d);
      } else if (d.type() == SchedulingDelta::PREEMPT) {
        HandlePreemptionDelta(d);
      } else if (d.type() == SchedulingDelta::MIGRATE) {
        HandleMigrationDelta(d);
      } else if (d.type() == SchedulingDelta::NOOP) {
        // We do not have to do anything.
      } else {
        LOG(FATAL) << "Encountered unsupported scheduling delta of type "
                   << to_string(d.type());
      }
    }
    return Status::OK;
  }

  // Pod affinity/anti-affinity
  void RemoveTaskFromLabelsMap(const TaskDescriptor td) {
    for (const auto& label : td.labels()) {
      unordered_map<string, vector<TaskID_t>>* label_values =
          FindOrNull(labels_map_, label.key());
      if (label_values) {
        vector<TaskID_t>* labels_map_tasks =
            FindOrNull(*label_values, label.value());
        if (labels_map_tasks) {
          vector<TaskID_t>::iterator it_pos = find(
              labels_map_tasks->begin(), labels_map_tasks->end(), td.uid());
          if (it_pos != labels_map_tasks->end()) {
            labels_map_tasks->erase(it_pos);
            if (!labels_map_tasks->size()) {
              label_values->erase(label.value());
              if (label_values->empty()) labels_map_.erase(label.key());
            }
          }
        }
      }
    }
    if (td.has_affinity() && (td.affinity().has_pod_affinity() ||
                              td.affinity().has_pod_anti_affinity())) {
      vector<TaskID_t>::iterator it =
          find(affinity_antiaffinity_tasks_.begin(),
               affinity_antiaffinity_tasks_.end(), td.uid());
      if (it != affinity_antiaffinity_tasks_.end()) {
        affinity_antiaffinity_tasks_.erase(it);
      }
    }
  }

  Status TaskCompleted(ServerContext* context, const TaskUID* tid_ptr,
                       TaskCompletedResponse* reply) override {
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, tid_ptr->task_uid());
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }
    if (FLAGS_resource_stats_update_based_on_resource_reservation) {
      if (!td_ptr->scheduled_to_resource().empty()) {
        UpdateMachineSamplesToKnowledgeBaseStatically(td_ptr, true);
      }
    }
    JobID_t job_id = JobIDFromString(td_ptr->job_id());
    JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);
    if (jd_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_JOB_NOT_FOUND);
      return Status::OK;
    }
    td_ptr->set_finish_time(wall_time_.GetCurrentTimestamp());
    RemoveTaskFromLabelsMap(*td_ptr);
    TaskFinalReport report;
    scheduler_->HandleTaskCompletion(td_ptr, &report);
    kb_populator_->PopulateTaskFinalReport(*td_ptr, &report);
    scheduler_->HandleTaskFinalReport(report, td_ptr);
    // Check if it was the last task of the job.
    uint64_t* num_incomplete_tasks =
        FindOrNull(job_num_incomplete_tasks_, job_id);
    CHECK_NOTNULL(num_incomplete_tasks);
    CHECK_GE(*num_incomplete_tasks, 1);
    (*num_incomplete_tasks)--;
    if (*num_incomplete_tasks == 0) {
      scheduler_->HandleJobCompletion(job_id);
    }
    reply->set_type(TaskReplyType::TASK_COMPLETED_OK);
    return Status::OK;
  }

  Status TaskFailed(ServerContext* context, const TaskUID* tid_ptr,
                    TaskFailedResponse* reply) override {
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, tid_ptr->task_uid());
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }
    if (FLAGS_resource_stats_update_based_on_resource_reservation) {
      if (!td_ptr->scheduled_to_resource().empty()) {
        UpdateMachineSamplesToKnowledgeBaseStatically(td_ptr, true);
      }
    }
    scheduler_->HandleTaskFailure(td_ptr);
    reply->set_type(TaskReplyType::TASK_FAILED_OK);
    return Status::OK;
  }

  Status TaskRemoved(ServerContext* context, const TaskUID* tid_ptr,
                     TaskRemovedResponse* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(
        scheduler_->scheduling_lock_);
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, tid_ptr->task_uid());
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }
    RemoveTaskFromLabelsMap(*td_ptr);
    if (FLAGS_resource_stats_update_based_on_resource_reservation) {
      if (!(td_ptr->scheduled_to_resource().empty()) &&
          (td_ptr->state() != TaskDescriptor::COMPLETED) &&
          (td_ptr->state() != TaskDescriptor::FAILED)) {
        UpdateMachineSamplesToKnowledgeBaseStatically(td_ptr, true);
      }
    }
    scheduler_->HandleTaskRemoval(td_ptr);
    JobID_t job_id = JobIDFromString(td_ptr->job_id());
    JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);
    CHECK_NOTNULL(jd_ptr);
    // Don't remove the root task so that tasks can still be appended to
    // the job. We only remove the root task when the job completes.
    if (td_ptr != jd_ptr->mutable_root_task()) {
      task_map_->erase(td_ptr->uid());
    }
    uint64_t* num_tasks_to_remove =
        FindOrNull(job_num_tasks_to_remove_, job_id);
    CHECK_NOTNULL(num_tasks_to_remove);
    (*num_tasks_to_remove)--;
    if (*num_tasks_to_remove == 0) {
      uint64_t* num_incomplete_tasks =
          FindOrNull(job_num_incomplete_tasks_, job_id);
      if (*num_incomplete_tasks > 0) {
        scheduler_->HandleJobRemoval(job_id);
      }
      // Delete the job because we removed its last task.
      task_map_->erase(jd_ptr->root_task().uid());
      job_map_->erase(job_id);
      job_num_incomplete_tasks_.erase(job_id);
      job_num_tasks_to_remove_.erase(job_id);
    }
    reply->set_type(TaskReplyType::TASK_REMOVED_OK);
    return Status::OK;
  }

  // Pod affinity/anti-affinity
  // Adding labels of task to the labels_map_
  void AddTaskToLabelsMap(const TaskDescriptor& td) {
    TaskID_t task_id = td.uid();
    for (const auto& label : td.labels()) {
      unordered_map<string, vector<TaskID_t>>* label_values =
          FindOrNull(labels_map_, label.key());
      if (!label_values) {
        vector<TaskID_t> tasks;
        tasks.push_back(task_id);
        unordered_map<string, vector<TaskID_t>> values;
        CHECK(InsertIfNotPresent(&values, label.value(), tasks));
        CHECK(InsertIfNotPresent(&labels_map_, label.key(), values));
      } else {
        vector<TaskID_t>* labels_map_tasks =
            FindOrNull(*label_values, label.value());
        if (!labels_map_tasks) {
          vector<TaskID_t> value_tasks;
          value_tasks.push_back(task_id);
          CHECK(
              InsertIfNotPresent(&(*label_values), label.value(), value_tasks));
        } else {
          labels_map_tasks->push_back(task_id);
        }
      }
    }
    if (td.has_affinity() && (td.affinity().has_pod_affinity() ||
                              td.affinity().has_pod_anti_affinity())) {
      affinity_antiaffinity_tasks_.push_back(task_id);
    }
  }

  Status TaskSubmitted(ServerContext* context,
                       const TaskDescription* task_desc_ptr,
                       TaskSubmittedResponse* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(
        scheduler_->scheduling_lock_);
    TaskID_t task_id = task_desc_ptr->task_descriptor().uid();
    if (FindPtrOrNull(*task_map_, task_id)) {
      reply->set_type(TaskReplyType::TASK_ALREADY_SUBMITTED);
      return Status::OK;
    }
    if (task_desc_ptr->task_descriptor().state() != TaskDescriptor::CREATED) {
      reply->set_type(TaskReplyType::TASK_STATE_NOT_CREATED);
      return Status::OK;
    }
    AddTaskToLabelsMap(task_desc_ptr->task_descriptor());
    JobID_t job_id = JobIDFromString(task_desc_ptr->task_descriptor().job_id());
    JobDescriptor* jd_ptr = FindOrNull(*job_map_, job_id);
    if (jd_ptr == NULL) {
      CHECK(InsertIfNotPresent(job_map_.get(), job_id,
                               task_desc_ptr->job_descriptor()));
      jd_ptr = FindOrNull(*job_map_, job_id);
      TaskDescriptor* root_td_ptr = jd_ptr->mutable_root_task();
      // Task that comes first is made as root task of the job.
      // Root task that was set in poseidon is ignored.
      root_td_ptr->CopyFrom(task_desc_ptr->task_descriptor());
      CHECK(
          InsertIfNotPresent(task_map_.get(), root_td_ptr->uid(), root_td_ptr));
      root_td_ptr->set_submit_time(wall_time_.GetCurrentTimestamp());
      CHECK(InsertIfNotPresent(&job_num_incomplete_tasks_, job_id, 0));
      CHECK(InsertIfNotPresent(&job_num_tasks_to_remove_, job_id, 0));
    } else {
      TaskDescriptor* td_ptr = jd_ptr->mutable_root_task()->add_spawned();
      td_ptr->CopyFrom(task_desc_ptr->task_descriptor());
      CHECK(InsertIfNotPresent(task_map_.get(), td_ptr->uid(), td_ptr));
      td_ptr->set_submit_time(wall_time_.GetCurrentTimestamp());
    }
    uint64_t* num_incomplete_tasks =
        FindOrNull(job_num_incomplete_tasks_, job_id);
    CHECK_NOTNULL(num_incomplete_tasks);
    if (*num_incomplete_tasks == 0) {
      scheduler_->AddJob(jd_ptr);
    }
    (*num_incomplete_tasks)++;
    uint64_t* num_tasks_to_remove =
        FindOrNull(job_num_tasks_to_remove_, job_id);
    (*num_tasks_to_remove)++;
    reply->set_type(TaskReplyType::TASK_SUBMITTED_OK);
    return Status::OK;
  }

  Status TaskUpdated(ServerContext* context,
                     const TaskDescription* task_desc_ptr,
                     TaskUpdatedResponse* reply) override {
    TaskID_t task_id = task_desc_ptr->task_descriptor().uid();
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }
    // The scheduler will notice that the task's properties (e.g.,
    // resource requirements, labels) are different and react accordingly.
    const TaskDescriptor& updated_td = task_desc_ptr->task_descriptor();
    td_ptr->mutable_resource_request()->CopyFrom(updated_td.resource_request());
    td_ptr->set_priority(updated_td.priority());
    td_ptr->clear_labels();
    for (const auto& label : updated_td.labels()) {
      Label* label_ptr = td_ptr->add_labels();
      label_ptr->CopyFrom(label);
    }
    td_ptr->clear_label_selectors();
    for (const auto& label_selector : updated_td.label_selectors()) {
      LabelSelector* label_sel_ptr = td_ptr->add_label_selectors();
      label_sel_ptr->CopyFrom(label_selector);
    }
    // XXX(ionel): We may want to add support for other field updates as well.
    reply->set_type(TaskReplyType::TASK_UPDATED_OK);
    return Status::OK;
  }

  bool CheckResourceDoesntExist(const ResourceDescriptor& rd) {
    ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, ResourceIDFromString(rd.uuid()));
    return rs_ptr == NULL;
  }

  void AddResource(ResourceTopologyNodeDescriptor* rtnd_ptr) {
    ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
    ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
    ResourceStatus* rs_ptr =
        new ResourceStatus(rd_ptr, rtnd_ptr, rd_ptr->friendly_name(), 0);
    CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));
  }

  Status NodeAdded(ServerContext* context,
                   const ResourceTopologyNodeDescriptor* submitted_rtnd_ptr,
                   NodeAddedResponse* reply) override {
    boost::lock_guard<boost::recursive_mutex> lock(
        scheduler_->scheduling_lock_);
    bool doesnt_exist = DFSTraverseResourceProtobufTreeWhileTrue(
        *submitted_rtnd_ptr,
        boost::bind(&FirmamentSchedulerServiceImpl::CheckResourceDoesntExist,
                    this, _1));
    if (!doesnt_exist) {
      reply->set_type(NodeReplyType::NODE_ALREADY_EXISTS);
      return Status::OK;
    }
    ResourceStatus* root_rs_ptr =
        FindPtrOrNull(*resource_map_, top_level_res_id_);
    CHECK_NOTNULL(root_rs_ptr);
    ResourceTopologyNodeDescriptor* rtnd_ptr =
        root_rs_ptr->mutable_topology_node()->add_children();
    rtnd_ptr->CopyFrom(*submitted_rtnd_ptr);
    rtnd_ptr->set_parent_id(to_string(top_level_res_id_));
    DFSTraverseResourceProtobufTreeReturnRTND(
        rtnd_ptr,
        boost::bind(&FirmamentSchedulerServiceImpl::AddResource, this, _1));
    // TODO(ionel): we use a hack here -- we pass simulated=true to
    // avoid Firmament instantiating an actual executor for this resource.
    // Instead, we rely on the no-op SimulatedExecutor. We should change
    // it such that Firmament does not mandatorily create an executor.
    scheduler_->RegisterResource(rtnd_ptr, false, true);
    reply->set_type(NodeReplyType::NODE_ADDED_OK);

    if (FLAGS_resource_stats_update_based_on_resource_reservation) {
      // Add Node initial status simulation
      ResourceStats resource_stats;
      ResourceID_t res_id =
          ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
      ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
      if (rs_ptr == NULL || rs_ptr->mutable_descriptor() == NULL) {
        reply->set_type(NodeReplyType::NODE_NOT_FOUND);
        return Status::OK;
      }
      resource_stats.set_resource_id(rtnd_ptr->resource_desc().uuid());
      resource_stats.set_timestamp(0);

      CpuStats* cpu_stats = resource_stats.add_cpus_stats();
      // As some of the resources is utilized by system pods, so initializing
      // utilization to 10%.
      cpu_stats->set_cpu_capacity(
          rtnd_ptr->resource_desc().resource_capacity().cpu_cores());
      cpu_stats->set_cpu_utilization(0.1);
      cpu_stats->set_cpu_allocatable(cpu_stats->cpu_capacity() * 0.9);
      resource_stats.set_mem_capacity(
          rtnd_ptr->resource_desc().resource_capacity().ram_cap());
      resource_stats.set_mem_utilization(0.1);
      resource_stats.set_mem_allocatable(resource_stats.mem_capacity() * 0.9);
      resource_stats.set_disk_bw(0);
      resource_stats.set_net_rx_bw(0);
      resource_stats.set_net_tx_bw(0);
      knowledge_base_->AddMachineSample(resource_stats);
    }
    return Status::OK;
  }

  Status NodeFailed(ServerContext* context, const ResourceUID* rid_ptr,
                    NodeFailedResponse* reply) override {
    ResourceID_t res_id = ResourceIDFromString(rid_ptr->resource_uid());
    ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    if (rs_ptr == NULL) {
      reply->set_type(NodeReplyType::NODE_NOT_FOUND);
      return Status::OK;
    }
    scheduler_->DeregisterResource(rs_ptr->mutable_topology_node());
    reply->set_type(NodeReplyType::NODE_FAILED_OK);
    return Status::OK;
  }

  Status NodeRemoved(ServerContext* context, const ResourceUID* rid_ptr,
                     NodeRemovedResponse* reply) override {
    ResourceID_t res_id = ResourceIDFromString(rid_ptr->resource_uid());
    ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    if (rs_ptr == NULL) {
      reply->set_type(NodeReplyType::NODE_NOT_FOUND);
      return Status::OK;
    }
    scheduler_->DeregisterResource(rs_ptr->mutable_topology_node());
    reply->set_type(NodeReplyType::NODE_REMOVED_OK);
    return Status::OK;
  }

  Status NodeUpdated(ServerContext* context,
                     const ResourceTopologyNodeDescriptor* updated_rtnd_ptr,
                     NodeUpdatedResponse* reply) override {
    ResourceID_t res_id =
        ResourceIDFromString(updated_rtnd_ptr->resource_desc().uuid());
    ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    if (rs_ptr == NULL) {
      reply->set_type(NodeReplyType::NODE_NOT_FOUND);
      return Status::OK;
    }
    DFSTraverseResourceProtobufTreesReturnRTNDs(
        rs_ptr->mutable_topology_node(), *updated_rtnd_ptr,
        boost::bind(&FirmamentSchedulerServiceImpl::UpdateNodeLabels, this, _1,
                    _2));
		DFSTraverseResourceProtobufTreesReturnRTNDs(
		rs_ptr->mutable_topology_node(), *updated_rtnd_ptr,
		boost::bind(&FirmamentSchedulerServiceImpl::UpdateNodeTaints, this, _1,
                    _2));
    // TODO(ionel): Support other types of node updates.
    reply->set_type(NodeReplyType::NODE_UPDATED_OK);
    return Status::OK;
  }

  void UpdateNodeLabels(ResourceTopologyNodeDescriptor* old_rtnd_ptr,
                        const ResourceTopologyNodeDescriptor& new_rtnd_ptr) {
    ResourceDescriptor* old_rd_ptr = old_rtnd_ptr->mutable_resource_desc();
    const ResourceDescriptor& new_rd = new_rtnd_ptr.resource_desc();
    old_rd_ptr->clear_labels();
    for (const auto& label : new_rd.labels()) {
      Label* label_ptr = old_rd_ptr->add_labels();
      label_ptr->CopyFrom(label);
    }
  }
  
  void UpdateNodeTaints(ResourceTopologyNodeDescriptor* old_rtnd_ptr,
                        const ResourceTopologyNodeDescriptor& new_rtnd_ptr) {
	ResourceDescriptor* old_rd_ptr = old_rtnd_ptr->mutable_resource_desc();
    const ResourceDescriptor& new_rd = new_rtnd_ptr.resource_desc();	
	old_rd_ptr->clear_taints();
	for (const auto& taint : new_rd.taints()) {
		Taint* taint_ptr = old_rd_ptr->add_taints();
		taint_ptr->CopyFrom(taint);
    	}
	}

  Status AddTaskStats(ServerContext* context, const TaskStats* task_stats,
                      TaskStatsResponse* reply) override {
    TaskID_t task_id = task_stats->task_id();
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
    if (td_ptr == NULL) {
      reply->set_type(TaskReplyType::TASK_NOT_FOUND);
      return Status::OK;
    }
    knowledge_base_->AddTaskStatsSample(*task_stats);
    return Status::OK;
  }

  Status AddNodeStats(ServerContext* context,
                      const ResourceStats* resource_stats,
                      ResourceStatsResponse* reply) override {
    ResourceID_t res_id = ResourceIDFromString(resource_stats->resource_id());
    ResourceStatus* rs_ptr = FindPtrOrNull(*resource_map_, res_id);
    if (rs_ptr == NULL || rs_ptr->mutable_descriptor() == NULL) {
      reply->set_type(NodeReplyType::NODE_NOT_FOUND);
      return Status::OK;
    }
    knowledge_base_->AddMachineSample(*resource_stats);
    return Status::OK;
  }

  Status Check(ServerContext* context, const HealthCheckRequest* health_service,
               HealthCheckResponse* reply) override {
    if (health_service->grpc_service().empty()) {
      reply->set_status(ServingStatus::SERVING);
    }
    return Status::OK;
  }

 private:
  SchedulerInterface* scheduler_;
  SimulatedMessagingAdapter<BaseMessage>* sim_messaging_adapter_;
  TraceGenerator* trace_generator_;
  WallTime wall_time_;
  // Data structures thare are populated by the scheduler. The service should
  // never have to directly insert values in these data structures.
  boost::shared_ptr<JobMap_t> job_map_;
  boost::shared_ptr<KnowledgeBase> knowledge_base_;
  boost::shared_ptr<ObjectStoreInterface> obj_store_;
  boost::shared_ptr<TaskMap_t> task_map_;
  boost::shared_ptr<TopologyManager> topology_manager_;
  // Data structures that we populate in the scheduler service.
  boost::shared_ptr<ResourceMap_t> resource_map_;
  ResourceID_t top_level_res_id_;
  // Mapping from JobID_t to number of incomplete job tasks.
  unordered_map<JobID_t, uint64_t, boost::hash<boost::uuids::uuid>>
      job_num_incomplete_tasks_;
  // Mapping from JobID_t to number of job tasks left to be removed.
  unordered_map<JobID_t, uint64_t, boost::hash<boost::uuids::uuid>>
      job_num_tasks_to_remove_;
  KnowledgeBasePopulator* kb_populator_;
  // Pod affinity/anti-affinity
  unordered_map<string, unordered_map<string, vector<TaskID_t>>> labels_map_;
  vector<TaskID_t> affinity_antiaffinity_tasks_;

  ResourceStatus* CreateTopLevelResource() {
    ResourceID_t res_id = GenerateResourceID();
    ResourceTopologyNodeDescriptor* rtnd_ptr =
        new ResourceTopologyNodeDescriptor();
    // Set up the RD
    ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
    rd_ptr->set_uuid(to_string(res_id));
    rd_ptr->set_type(ResourceDescriptor::RESOURCE_COORDINATOR);
    // Need to maintain a ResourceStatus for the resource map
    ResourceStatus* rs_ptr =
        new ResourceStatus(rd_ptr, rtnd_ptr, "root_resource", 0);
    // Insert into resource map
    CHECK(InsertIfNotPresent(resource_map_.get(), res_id, rs_ptr));
    return rs_ptr;
  }
  boost::recursive_mutex task_submission_lock_;
  boost::recursive_mutex node_addition_lock_;
};

}  // namespace firmament

int main(int argc, char* argv[]) {
  VLOG(1) << "Calling common::InitFirmament";
  firmament::common::InitFirmament(argc, argv);
  std::string server_address(FLAGS_firmament_scheduler_service_address + ":" +
                             FLAGS_firmament_scheduler_service_port);
  LOG(INFO) << "Firmament scheduler starting ...";
  firmament::FirmamentSchedulerServiceImpl scheduler;
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&scheduler);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  LOG(INFO) << "Firmament scheduler listening on " << server_address;
  server->Wait();
  return 0;
}
