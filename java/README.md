[![Build Status](https://jenkins.cloudaware.com/buildStatus/icon?style=plastic&job=appengine-pipeline)](https://jenkins.cloudaware.com/job/appengine-pipeline)
# About

This version of appengine-pipeline library adapted for [AppEngine Flexible](https://cloud.google.com/appengine/docs/flexible/) environment.
  
It uses following libraries:

* Google Cloud Datastore [Java Client](https://cloud.google.com/datastore/docs/reference/libraries#client-libraries-install-java) (instead of AppEngine Standard `DatastoreServiceFactory`)
* Google Cloud Tasks Java Client (instead of AppEngine Standard `QueueFactory`)
* Flex-compatible implementation of [Deferred Tasks](https://github.com/cloudaware/deferred)

# Configuration

There are 2 implementation of `PipelineTaskQueue` interface:
 
* `AppEngineTaskQueue` - implementation for Standard environment
* `CloudTaskQueue` - implementation that uses Cloud Tasks client library

To use `CloudTaskQueue` you need to set following properties:

* `cloudtasks.api.root.url` - Cloud Tasks api URL, by default use `https://cloudtasks.googleapis.com/`
* Optionally `cloudtasks.api.key` - If you use [local implementation](https://github.com/cloudaware/cloudmine-appengine) of Cloud Tasks use this property to set up API Key to your API
* `cloudtasks.api.default.parent` - Used to construct correct object names for Cloud Tasks, should use following pattern `projects/{applicationId}/locations/{locationId}`
* `clouddatastore.api.root.url` - If you want to use local Cloud Datastore emulator, e.g. `http://localhost:8080`
* `clouddatastore.project.id` - for the Cloud Datastore too

Pass properties with your `app.yaml`:

```yaml
env_variables:
  JAVA_OPTS: >-
    -Dcloudtasks.api.root.url=https://cloudtasks.googleapis.com/
    -Dcloudtasks.api.default.parent=projects/yourapp/locations/us-central1
```

# Changelog

* 0.2.13.11.CLOUDTASKS-SNAPSHOT - fix scheduleTime, new version of deferred 
* 0.2.13.10.CLOUDTASKS-SNAPSHOT - support for millis in scheduleTime 
* 0.2.13.9.CLOUDTASKS-SNAPSHOT - alternative datastore implementation using MongoDB
* 0.2.13.8.CLOUDTASKS-SNAPSHOT - deferred update
* 0.2.13.7.CLOUDTASKS-SNAPSHOT - deferred update, deprecated refactoring
* 0.2.13.6.CLOUDTASKS-SNAPSHOT - deferred update, task RetryConfig support removed
* 0.2.13.5.CLOUDTASKS-SNAPSHOT - updated to latest Cloud Tasks version
* 0.2.13.4.CLOUDTASKS-SNAPSHOT - `com.cloudaware:deferred` update to 1.0.1 
* 0.2.13.3.CLOUDTASKS-SNAPSHOT - property `clouddatastore.project.id` added, if default credentials failed fallback to no credentials 
* 0.2.13.2.CLOUDTASKS-SNAPSHOT - property `clouddatastore.api.root.url` added
* 0.2.13.1.CLOUDTASKS-SNAPSHOT - `google-cloud-datastore` updated to 1.0.1 
* 0.2.13.CLOUDTASKS-SNAPSHOT - Cloud Tasks and Cloud Datastore release 
