CREATE TABLE Pipeline (
	pipelineKey STRING(36) NOT NULL,
	rootJobClassName STRING(255),
	rootJobDisplayName STRING(255),
	startTime TIMESTAMP NOT NULL,
) PRIMARY KEY (pipelineKey)

CREATE NULL_FILTERED INDEX PipelineClassName
ON Pipeline (
	rootJobClassName
);

CREATE NULL_FILTERED INDEX PipelineDisplayName
ON Pipeline (
	rootJobDisplayName
);

CREATE TABLE Barrier (
	pipelineKey STRING(36) NOT NULL,
	key STRING(36) NOT NULL,
	barrierType STRING(16) NOT NULL,
	generatorJobKey STRING(36),
	graphKey STRING(36),
	jobKey STRING(36) NOT NULL,
	released BOOL NOT NULL,
	waitingOnGroupSizes ARRAY<INT64>,
	waitingOnKeys ARRAY<STRING(73)>,
) PRIMARY KEY (pipelineKey, key),
INTERLEAVE IN PARENT Pipeline ON DELETE CASCADE;

CREATE TABLE Exception (
	pipelineKey STRING(36) NOT NULL,
	key STRING(36) NOT NULL,
	exceptionBytes BYTES(MAX) NOT NULL,
	generatorJobKey STRING(36),
	graphKey STRING(36),
) PRIMARY KEY (pipelineKey, key),
INTERLEAVE IN PARENT Pipeline ON DELETE CASCADE;

CREATE TABLE FanoutTask (
	pipelineKey STRING(36) NOT NULL,
	key STRING(36) NOT NULL,
	generatorJobKey STRING(36),
	graphKey STRING(36),
	payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (pipelineKey, key),
INTERLEAVE IN PARENT Pipeline ON DELETE CASCADE;

CREATE TABLE Job (
	pipelineKey STRING(36) NOT NULL,
	key STRING(36) NOT NULL,
	attemptNum INT64,
	backoffFactor INT64 NOT NULL,
	backoffSeconds INT64 NOT NULL,
	callExceptionHandler BOOL,
	childGraphKey STRING(36),
	childKeys ARRAY<STRING(36)>,
	endTime TIMESTAMP,
	exceptionHandlerJobGraphKey STRING(36),
	exceptionHandlerJobKey STRING(36),
	exceptionHandlingAncestorKey STRING(36),
	exceptionKey STRING(36),
	finalizeBarrier STRING(36) NOT NULL,
	generatorJobKey STRING(36),
	graphKey STRING(36),
	hasExceptionHandler BOOL,
	ignoreException BOOL,
	jobInstance STRING(36) NOT NULL,
	maxAttempts INT64 NOT NULL,
	onQueue STRING(255),
	outputSlot STRING(36) NOT NULL,
	route STRING(1024),
	runBarrier STRING(36) NOT NULL,
	startTime TIMESTAMP,
	state STRING(32) NOT NULL,
	statusConsoleUrl STRING(255),
	statusMessages ARRAY<STRING(1000)>,
) PRIMARY KEY (pipelineKey, key),
INTERLEAVE IN PARENT Pipeline ON DELETE CASCADE;

CREATE TABLE JobInstance (
	pipelineKey STRING(36) NOT NULL,
	key STRING(36) NOT NULL,
	databaseValue BYTES(MAX),
	generatorJobKey STRING(36),
	graphKey STRING(36),
	jobClassName STRING(255) NOT NULL,
	jobDisplayName STRING(255),
	jobKey STRING(36) NOT NULL,
	valueLocation STRING(32),
) PRIMARY KEY (pipelineKey, key),
INTERLEAVE IN PARENT Pipeline ON DELETE CASCADE;

CREATE TABLE Slot (
	pipelineKey STRING(36) NOT NULL,
	key STRING(36) NOT NULL,
	databaseValue BYTES(MAX),
	filled BOOL,
	fillTime TIMESTAMP,
	generatorJobKey STRING(36),
	graphKey STRING(36),
	sourceJob STRING(36),
	valueLocation STRING(32),
	waitingOnMe ARRAY<STRING(73)>,
) PRIMARY KEY (pipelineKey, key),
INTERLEAVE IN PARENT Pipeline ON DELETE CASCADE;

