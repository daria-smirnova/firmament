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

size_t HashAffinity(const Affinity& affinity) {
  size_t seed = 0;
  LOG(INFO) << "DEBUG: Affinity is set";
  if (affinity.has_node_affinity()) {
    LOG(INFO) << "DEBUG: NodeAffinity is set";
    if (affinity.node_affinity()
            .has_requiredduringschedulingignoredduringexecution()) {
      LOG(INFO) << "DEBUG: NodeSelector(requiredduringscheduling) is set";
      auto node_selector =
          affinity.node_affinity()
              .requiredduringschedulingignoredduringexecution();
      if (node_selector.nodeselectorterms_size()) {
        LOG(INFO) << "DEBUG: Total number of node Selector terms: "
                  << (node_selector.nodeselectorterms_size());
        auto selector_terms = node_selector.nodeselectorterms();
        for (auto& it : selector_terms) {
          if (it.matchexpressions_size()) {
            LOG(INFO) << "DEBUG: Total number of match expressions: "
                      << it.matchexpressions_size();
            for (auto& it1 : it.matchexpressions()) {
              LOG(INFO) << "DEBUG: Key: " << it1.key();
              LOG(INFO) << "DEBUG: Operator " << it1.operator_();
              boost::hash_combine(seed, HashString(it1.key()));
              boost::hash_combine(seed, HashString(it1.operator_()));
              for (auto& it2 : it1.values()) {
                LOG(INFO) << "DEBUG: Value " << it2;
                boost::hash_combine(seed, HashString(it2));
              }
            }
          }
        }
      }
    }
    // Preferred now
    if (affinity.node_affinity()
            .preferredduringschedulingignoredduringexecution_size()) {
      LOG(INFO) << "DEBUG: preferred duringscheduling is set with size"
                << affinity.node_affinity()
                       .preferredduringschedulingignoredduringexecution_size();
      for (auto& it : affinity.node_affinity()
                          .preferredduringschedulingignoredduringexecution()) {
        LOG(INFO) << "DEBUG: Weight" << it.weight();
        LOG(INFO) << "DEBUG: preferred matchexpressions size: "
                  << it.preference().matchexpressions_size();
        for (auto& it1 : it.preference().matchexpressions()) {
          LOG(INFO) << "DEBUG: Key: " << it1.key();
          LOG(INFO) << "DEBUG: Operator " << it1.operator_();
          boost::hash_combine(seed, HashString(it1.key()));
          boost::hash_combine(seed, HashString(it1.operator_()));
          for (auto& it2 : it1.values()) {
            LOG(INFO) << "DEBUG: Value " << it2;
            boost::hash_combine(seed, HashString(it2));
          }
        }
      }
    }
  } else {
    LOG(INFO) << "DEBUG: NodeAffinity is NOT set";
  }
  return seed;
}

RepeatedPtrField<LabelSelector> NodeSelectorRequirementsAsLabelSelectors(const RepeatedPtrField<NodeSelectorRequirement>& matchExpressions) {
  TaskDescriptor td;
  for (auto & nsm : matchExpressions) {
    LabelSelector selector;
    selector.set_key(nsm.key());
    uint64_t type;
    string operator_type = nsm.operator_();
    if (operator_type == "In")
      type = 0;
    else if (operator_type == "NotIn")
      type = 1;
    else if (operator_type == "Exists")
      type = 2;
    else if (operator_type == "DoesNotExist")
      type = 3;
    selector.set_type(static_cast<LabelSelector_SelectorType>(type));
    for (auto & value : nsm.values()) {
      selector.add_values(value);
    }
    LabelSelector* label_selector_ptr = td.add_label_selectors();
    label_selector_ptr->CopyFrom(selector);
  }
  return td.label_selectors();
}

bool SatisfiesMatchExpressions(const ResourceDescriptor& rd, const RepeatedPtrField<NodeSelectorRequirement>& matchExpressions) {
  const RepeatedPtrField<LabelSelector>& selectors = NodeSelectorRequirementsAsLabelSelectors(matchExpressions);
  return SatisfiesLabelSelectors(rd, selectors);
}

bool NodeMatchesNodeSelectorTerms(const ResourceDescriptor& rd, const RepeatedPtrField<NodeSelectorTerm>& nodeSelectorTerms) {
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

bool SatisfiesAffinity(const ResourceDescriptor& rd, const Affinity& affinity) {
  bool nodeAffinityMatches = true;
  if (affinity.has_node_affinity()) {
   if (affinity.node_affinity().has_requiredduringschedulingignoredduringexecution()) {
     if (affinity.node_affinity().requiredduringschedulingignoredduringexecution().nodeselectorterms_size()) {
       // Match node selector for requiredDuringSchedulingIgnoredDuringExecution.
       auto nodeSelectorTerms = affinity.node_affinity().requiredduringschedulingignoredduringexecution().nodeselectorterms();
       nodeAffinityMatches = nodeAffinityMatches && NodeMatchesNodeSelectorTerms(rd, nodeSelectorTerms);
    }
   } else {
     // if no required NodeAffinity requirements, will do no-op, means select all nodes.
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

}  // namespace scheduler
}  // namespace firmament
