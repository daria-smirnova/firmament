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

#include <boost/functional/hash.hpp>
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/label_utils.h"

namespace firmament {
namespace scheduler {

RepeatedPtrField<LabelSelector> NodeSelectorRequirementsAsLabelSelectors(
    const RepeatedPtrField<NodeSelectorRequirement>& matchExpressions) {
  TaskDescriptor td;
  for (auto& nsm : matchExpressions) {
    LabelSelector selector;
    selector.set_key(nsm.key());
    uint64_t type = 0;
    string operator_type = nsm.operator_();
    if (operator_type == "In")
      type = 0;
    else if (operator_type == "NotIn")
      type = 1;
    else if (operator_type == "Exists")
      type = 2;
    else if (operator_type == "DoesNotExist")
      type = 3;
    else if (operator_type == "Gt")
      type = 4;
    else if (operator_type == "Lt")
      type = 5;
    selector.set_type(static_cast<LabelSelector_SelectorType>(type));
    for (auto& value : nsm.values()) {
      selector.add_values(value);
    }
    LabelSelector* label_selector_ptr = td.add_label_selectors();
    label_selector_ptr->CopyFrom(selector);
  }
  return td.label_selectors();
}

bool SatisfiesMatchExpressions(
    const ResourceDescriptor& rd,
    const RepeatedPtrField<NodeSelectorRequirement>& matchExpressions) {
  const RepeatedPtrField<LabelSelector>& selectors =
      NodeSelectorRequirementsAsLabelSelectors(matchExpressions);
  return SatisfiesLabelSelectors(rd, selectors);
}

bool NodeMatchesNodeSelectorTerm(const ResourceDescriptor& rd,
                                 const NodeSelectorTerm& nodeSelectorTerm) {
  if (nodeSelectorTerm.matchexpressions_size() == 0) {
    return false;
  } else {
    return SatisfiesMatchExpressions(rd, nodeSelectorTerm.matchexpressions());
  }
}

bool NodeMatchesNodeSelectorTerms(
    const ResourceDescriptor& rd,
    const RepeatedPtrField<NodeSelectorTerm>& nodeSelectorTerms) {
  for (auto& req : nodeSelectorTerms) {
    if (req.matchexpressions_size() == 0) {
      continue;
    }
    if (!SatisfiesMatchExpressions(rd, req.matchexpressions())) {
      continue;
    }
    return true;
  }
  return false;
}

bool SatisfiesNodeSelectorAndNodeAffinity(const ResourceDescriptor& rd, const TaskDescriptor& td) {
  if (td.label_selectors_size()) {
    if (!SatisfiesLabelSelectors(rd, td.label_selectors())) {
      return false;
    } 
  }
  bool nodeAffinityMatches = true;
  if (td.has_affinity() && td.affinity().has_node_affinity()) {
    const Affinity& affinity = td.affinity();
    if (affinity.node_affinity()
            .has_requiredduringschedulingignoredduringexecution()) {
      if (affinity.node_affinity()
              .requiredduringschedulingignoredduringexecution()
              .nodeselectorterms_size()) {
        // Match node selector for
        // requiredDuringSchedulingIgnoredDuringExecution.
        auto nodeSelectorTerms =
            affinity.node_affinity()
                .requiredduringschedulingignoredduringexecution()
                .nodeselectorterms();
        nodeAffinityMatches =
            nodeAffinityMatches &&
            NodeMatchesNodeSelectorTerms(rd, nodeSelectorTerms);
      }
    } else {
      // if no required NodeAffinity requirements, will do no-op, means select
      // all nodes.
      return true;
    }
  }
  return nodeAffinityMatches;
}

bool SatisfiesLabelSelectors(const ResourceDescriptor& rd,
                             const RepeatedPtrField<LabelSelector>& selectors) {
  unordered_map<string, string> rd_labels;
  for (const auto& label : rd.labels()) {
    InsertIfNotPresent(&rd_labels, label.key(), label.value());
  }
  for (auto& selector : selectors) {
    if (!SatisfiesLabelSelector(rd_labels, selector)) {
      return false;
    }
  }
  return true;
}

bool SatisfiesLabelSelector(const ResourceDescriptor& rd,
                            const LabelSelector& selector) {
  unordered_map<string, string> rd_labels;
  for (const auto& label : rd.labels()) {
    InsertIfNotPresent(&rd_labels, label.key(), label.value());
  }
  return SatisfiesLabelSelector(rd_labels, selector);
}

bool SatisfiesLabelSelector(const unordered_map<string, string>& rd_labels,
                            const LabelSelector& selector) {
  unordered_set<string> selector_values;
  for (const auto& value : selector.values()) {
    selector_values.insert(value);
  }
  return SatisfiesLabelSelector(rd_labels, selector_values, selector);
}

bool SatisfiesLabelSelector(const unordered_map<string, string>& rd_labels,
                            const unordered_set<string>& selector_values,
                            const LabelSelector& selector) {
  switch (selector.type()) {
    case LabelSelector::IN_SET: {
      const string* value = FindOrNull(rd_labels, selector.key());
      if (value != NULL) {
        if (selector_values.find(*value) != selector_values.end()) {
          return true;
        }
      }
      return false;
    }
    case LabelSelector::NOT_IN_SET: {
      const string* value = FindOrNull(rd_labels, selector.key());
      if (value != NULL) {
        if (selector_values.find(*value) != selector_values.end()) {
          return false;
        }
      }
      return true;
    }
    case LabelSelector::EXISTS_KEY: {
      return ContainsKey(rd_labels, selector.key());
    }
    case LabelSelector::NOT_EXISTS_KEY: {
      return !ContainsKey(rd_labels, selector.key());
    }
    case LabelSelector::GREATER_THAN: {
      const string* value = FindOrNull(rd_labels, selector.key());
      if (value != NULL) {
        return (stoi(*value) > stoi(*selector_values.begin()));
      }
      return false;
    }
    case LabelSelector::LESSER_THAN: {
      const string* value = FindOrNull(rd_labels, selector.key());
      if (value != NULL) {
        return (stoi(*value) < stoi(*selector_values.begin()));
      }
      return false;
    }
    default:
      LOG(FATAL) << "Unsupported selector type: " << selector.type();
  }
  return false;
}

size_t HashSelectors(const RepeatedPtrField<LabelSelector>& selectors) {
  size_t seed = 0;
  for (auto label_selector : selectors) {
    boost::hash_combine(seed, HashString(label_selector.key()));
    for (auto value : label_selector.values()) {
      boost::hash_combine(seed, HashString(value));
    }
  }
  return seed;
}

bool HasMatchingTolerationforNodeTaints(const ResourceDescriptor& rd,
                                        const TaskDescriptor& td) {
  bool IsPodScheduleOnNode = true;
  bool IsTolerable = false;
  unordered_map<string, string> tolerationHardEqualMap;
  unordered_map<string, string> tolerationHardExistsMap;
  tolerationHardEqualMap.clear();
  tolerationHardExistsMap.clear();
  if (rd.taints_size() && (td.tolerations_size() > DEFAULT_TOLERATIONS)) {
    for (const auto& tolerations : td.tolerations()) {
      // If it is hard constraint ie "NoSchedule" or "NoExecute", store all the
      // tolerations
      // of a given pod in a map
      // If there is a pod with nil effect, pod is tolerable to any taint on
      // node
      if (tolerations.effect() == "NoSchedule" ||
          tolerations.effect() == "NoExecute" || tolerations.effect() == "") {
        if (tolerations.operator_() == "Exists") {
          if (tolerations.key() != "") {
            if (tolerations.effect() != "") {
              InsertIfNotPresent(&tolerationHardExistsMap,
                                 tolerations.key() + tolerations.effect(),
                                 tolerations.value());
            } else {
              InsertIfNotPresent(&tolerationHardExistsMap,
                                 tolerations.key() + "NoExecute",
                                 tolerations.value());
              InsertIfNotPresent(&tolerationHardExistsMap,
                                 tolerations.key() + "NoSchedule",
                                 tolerations.value());
            }
          } else {
            IsTolerable = true;
          }
        } else if (tolerations.operator_() == "Equal") {
          if (tolerations.effect() != "") {
            InsertIfNotPresent(&tolerationHardEqualMap,
                               tolerations.key() + tolerations.effect(),
                               tolerations.value());
          } else {
            InsertIfNotPresent(&tolerationHardEqualMap,
                               tolerations.key() + "NoExecute",
                               tolerations.value());
            InsertIfNotPresent(&tolerationHardEqualMap,
                               tolerations.key() + "NoSchedule",
                               tolerations.value());
          }

        } else {
          LOG(FATAL) << "Unsupported operator :" << tolerations.operator_();
        }
      }
    }
  }
  if (!IsTolerable) {
    // Check if all the tolerations in Pod exists for all the taints on Node
    for (const auto& taint : rd.taints()) {
      if (taint.effect() == "NoSchedule" || taint.effect() == "NoExecute") {
        if (td.tolerations_size() == DEFAULT_TOLERATIONS) {
          // If the number of taints is more than 0 and there are no
          // tolerations, pods cannot be scheduled
          return !IsPodScheduleOnNode;
        }
        if (!ContainsKey(tolerationHardExistsMap,
                         taint.key() + taint.effect())) {
          const string* value = FindOrNull(tolerationHardEqualMap,
                                           (taint.key() + taint.effect()));

          if (value != NULL) {
            if (*value != taint.value()) {
              return !IsPodScheduleOnNode;
            }

          } else {
            return !IsPodScheduleOnNode;
          }
        }
      }
    }
  }
  return IsPodScheduleOnNode;
}

}  // namespace scheduler
}  // namespace firmament
