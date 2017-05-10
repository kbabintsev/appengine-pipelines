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

Pass properties with your `app.yaml`:

```yaml
env_variables:
  JAVA_OPTS: >-
    -Dcloudtasks.api.root.url=https://cloudtasks.googleapis.com/
    -Dcloudtasks.api.default.parent=projects/yourapp/locations/us-central1
```