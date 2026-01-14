# DeltaLakeTable

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**MetadataLocationHistory** | **[]string** |  | 
**CheckpointLocationHistory** | **[]string** |  | 
**LastCheckpoint** | Pointer to **string** |  | [optional] 

## Methods

### NewDeltaLakeTable

`func NewDeltaLakeTable(metadataLocationHistory []string, checkpointLocationHistory []string, ) *DeltaLakeTable`

NewDeltaLakeTable instantiates a new DeltaLakeTable object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDeltaLakeTableWithDefaults

`func NewDeltaLakeTableWithDefaults() *DeltaLakeTable`

NewDeltaLakeTableWithDefaults instantiates a new DeltaLakeTable object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *DeltaLakeTable) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *DeltaLakeTable) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *DeltaLakeTable) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *DeltaLakeTable) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocationHistory

`func (o *DeltaLakeTable) GetMetadataLocationHistory() []string`

GetMetadataLocationHistory returns the MetadataLocationHistory field if non-nil, zero value otherwise.

### GetMetadataLocationHistoryOk

`func (o *DeltaLakeTable) GetMetadataLocationHistoryOk() (*[]string, bool)`

GetMetadataLocationHistoryOk returns a tuple with the MetadataLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocationHistory

`func (o *DeltaLakeTable) SetMetadataLocationHistory(v []string)`

SetMetadataLocationHistory sets MetadataLocationHistory field to given value.


### GetCheckpointLocationHistory

`func (o *DeltaLakeTable) GetCheckpointLocationHistory() []string`

GetCheckpointLocationHistory returns the CheckpointLocationHistory field if non-nil, zero value otherwise.

### GetCheckpointLocationHistoryOk

`func (o *DeltaLakeTable) GetCheckpointLocationHistoryOk() (*[]string, bool)`

GetCheckpointLocationHistoryOk returns a tuple with the CheckpointLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCheckpointLocationHistory

`func (o *DeltaLakeTable) SetCheckpointLocationHistory(v []string)`

SetCheckpointLocationHistory sets CheckpointLocationHistory field to given value.


### GetLastCheckpoint

`func (o *DeltaLakeTable) GetLastCheckpoint() string`

GetLastCheckpoint returns the LastCheckpoint field if non-nil, zero value otherwise.

### GetLastCheckpointOk

`func (o *DeltaLakeTable) GetLastCheckpointOk() (*string, bool)`

GetLastCheckpointOk returns a tuple with the LastCheckpoint field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLastCheckpoint

`func (o *DeltaLakeTable) SetLastCheckpoint(v string)`

SetLastCheckpoint sets LastCheckpoint field to given value.

### HasLastCheckpoint

`func (o *DeltaLakeTable) HasLastCheckpoint() bool`

HasLastCheckpoint returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


