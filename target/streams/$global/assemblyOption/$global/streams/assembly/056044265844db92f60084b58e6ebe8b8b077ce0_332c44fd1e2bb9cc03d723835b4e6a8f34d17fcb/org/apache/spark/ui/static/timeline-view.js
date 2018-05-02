/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function drawApplicationTimeline(groupArray, eventObjArray, startTime, offset) {
  var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);
  var container = $("#application-timeline")[0];
  var options = {
    groupOrder: function(a, b) {
      return a.value - b.value
    },
    editable: false,
    showCurrentTime: false,
    min: startTime,
    zoomable: false,
    moment: function (date) {
      return vis.moment(date).utcOffset(offset);
    }
  };

  var applicationTimeline = new vis.Timeline(container);
  applicationTimeline.setOptions(options);
  applicationTimeline.setGroups(groups);
  applicationTimeline.setItems(items);

  setupZoomable("#application-timeline-zoom-lock", applicationTimeline);
  setupExecutorEventAction();

  function setupJobEventAction() {
    $(".vis-item.vis-range.job.application-timeline-object").each(function() {
      var getSelectorForJobEntry = function(baseElem) {
        var jobIdText = $($(baseElem).find(".application-timeline-content")[0]).text();
        var jobId = jobIdText.match("\\(Job (\\d+)\\)$")[1];
       return "#job-" + jobId;
      };

      $(this).click(function() {
        var jobPagePath = $(getSelectorForJobEntry(this)).find("a.name-link").attr("href")
          window.location.href = jobPagePath
      });

      $(this).hover(
        function() {
          $(getSelectorForJobEntry(this)).addClass("corresponding-item-hover");
          $($(this).find("div.application-timeline-content")[0]).tooltip("show");
        },
        function() {
          $(getSelectorForJobEntry(this)).removeClass("corresponding-item-hover");
          $($(this).find("div.application-timeline-content")[0]).tooltip("hide");
        }
      );
    });
  }

  setupJobEventAction();

  $("span.expand-application-timeline").click(function() {
    var status = window.localStorage.getItem("expand-application-timeline") == "true";
    status = !status;

    $("#application-timeline").toggleClass('collapsed');

    // Switch the class of the arrow from open to closed.
    $(this).find('.expand-application-timeline-arrow').toggleClass('arrow-open');
    $(this).find('.expand-application-timeline-arrow').toggleClass('arrow-closed');

    window.localStorage.setItem("expand-application-timeline", "" + status);
  });
}

$(function (){
  if (window.localStorage.getItem("expand-application-timeline") == "true") {
    // Set it to false so that the click function can revert it
    window.localStorage.setItem("expand-application-timeline", "false");
    $("span.expand-application-timeline").trigger('click');
  }
});

function drawJobTimeline(groupArray, eventObjArray, startTime, offset) {
  var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);
  var container = $('#job-timeline')[0];
  var options = {
    groupOrder: function(a, b) {
      return a.value - b.value;
    },
    editable: false,
    showCurrentTime: false,
    min: startTime,
    zoomable: false,
    moment: function (date) {
      return vis.moment(date).utcOffset(offset);
    }
  };

  var jobTimeline = new vis.Timeline(container);
  jobTimeline.setOptions(options);
  jobTimeline.setGroups(groups);
  jobTimeline.setItems(items);

  setupZoomable("#job-timeline-zoom-lock", jobTimeline);
  setupExecutorEventAction();

  function setupStageEventAction() {
    $(".vis-item.vis-range.stage.job-timeline-object").each(function() {
      var getSelectorForStageEntry = function(baseElem) {
        var stageIdText = $($(baseElem).find(".job-timeline-content")[0]).text();
        var stageIdAndAttempt = stageIdText.match("\\(Stage (\\d+\\.\\d+)\\)$")[1].split(".");
        return "#stage-" + stageIdAndAttempt[0] + "-" + stageIdAndAttempt[1];
      };

      $(this).click(function() {
        var stagePagePath = $(getSelectorForStageEntry(this)).find("a.name-link").attr("href")
        window.location.href = stagePagePath
      });

      $(this).hover(
        function() {
          $(getSelectorForStageEntry(this)).addClass("corresponding-item-hover");
          $($(this).find("div.job-timeline-content")[0]).tooltip("show");
        },
        function() {
          $(getSelectorForStageEntry(this)).removeClass("corresponding-item-hover");
          $($(this).find("div.job-timeline-content")[0]).tooltip("hide");
        }
      );
    });
  }

  setupStageEventAction();

  $("span.expand-job-timeline").click(function() {
    var status = window.localStorage.getItem("expand-job-timeline") == "true";
    status = !status;

    $("#job-timeline").toggleClass('collapsed');

    // Switch the class of the arrow from open to closed.
    $(this).find('.expand-job-timeline-arrow').toggleClass('arrow-open');
    $(this).find('.expand-job-timeline-arrow').toggleClass('arrow-closed');

    window.localStorage.setItem("expand-job-timeline", "" + status);
  });
}

$(function (){
  if (window.localStorage.getItem("expand-job-timeline") == "true") {
    // Set it to false so that the click function can revert it
    window.localStorage.setItem("expand-job-timeline", "false");
    $("span.expand-job-timeline").trigger('click');
  }
});

function drawTaskAssignmentTimeline(groupArray, eventObjArray, minLaunchTime, maxFinishTime, offset) {
  var groups = new vis.DataSet(groupArray);
  var items = new vis.DataSet(eventObjArray);
  var container = $("#task-assignment-timeline")[0]
  var options = {
    groupOrder: function(a, b) {
      return a.value - b.value
    },
    editable: false,
    align: 'left',
    selectable: false,
    showCurrentTime: false,
    min: minLaunchTime,
    max: maxFinishTime,
    zoomable: false,
    moment: function (date) {
      return vis.moment(date).utcOffset(offset);
    }
  };

  var taskTimeline = new vis.Timeline(container)
  taskTimeline.setOptions(options);
  taskTimeline.setGroups(groups);
  taskTimeline.setItems(items);

  // If a user zooms while a tooltip is displayed, the user may zoom such that the cursor is no
  // longer over the task that the tooltip corresponds to. So, when a user zooms, we should hide
  // any currently displayed tooltips.
  var currentDisplayedTooltip = null;
  $("#task-assignment-timeline").on({
    "mouseenter": function() {
      currentDisplayedTooltip = this;
    },
    "mouseleave": function() {
      currentDisplayedTooltip = null;
    }
  }, ".task-assignment-timeline-content");
  taskTimeline.on("rangechange", function(prop) {
    if (currentDisplayedTooltip !== null) {
      $(currentDisplayedTooltip).tooltip("hide");
    }
  });

  setupZoomable("#task-assignment-timeline-zoom-lock", taskTimeline);

  $("span.expand-task-assignment-timeline").click(function() {
    var status = window.localStorage.getItem("expand-task-assignment-timeline") == "true";
    status = !status;

    $("#task-assignment-timeline").toggleClass("collapsed");

     // Switch the class of the arrow from open to closed.
    $(this).find(".expand-task-assignment-timeline-arrow").toggleClass("arrow-open");
    $(this).find(".expand-task-assignment-timeline-arrow").toggleClass("arrow-closed");

    window.localStorage.setItem("expand-task-assignment-timeline", "" + status);
  });
}

$(function (){
  if (window.localStorage.getItem("expand-task-assignment-timeline") == "true") {
    // Set it to false so that the click function can revert it
    window.localStorage.setItem("expand-task-assignment-timeline", "false");
    $("span.expand-task-assignment-timeline").trigger('click');
  }
});

function setupExecutorEventAction() {
  $(".vis-item.vis-box.executor").each(function () {
    $(this).hover(
      function() {
        $($(this).find(".executor-event-content")[0]).tooltip("show");
      },
      function() {
        $($(this).find(".executor-event-content")[0]).tooltip("hide");
      }
    );
  });
}

function setupZoomable(id, timeline) {
  $(id + ' > input[type="checkbox"]').click(function() {
    if (this.checked) {
      timeline.setOptions({zoomable: true});
    } else {
      timeline.setOptions({zoomable: false});
    }
  });

  $(id + " > span").click(function() {
    $(this).parent().find('input:checkbox').trigger('click');
  });
}
