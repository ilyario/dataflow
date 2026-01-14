# DeltaLakeTableV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**MetadataLocationHistory** | **[]string** |  | 
**CheckpointLocationHistory** | **[]string** |  | 
**LastCheckpoint** | Pointer to **string** |  | [optional] 

## Methods

### NewDeltaLakeTableV1

`func NewDeltaLakeTableV1(metadataLocationHistory []string, checkpointLocationHistory []string, ) *DeltaLakeTableV1`

NewDeltaLakeTableV1 instantiates a new DeltaLakeTableV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDeltaLakeTableV1WithDefaults

`func NewDeltaLakeTableV1WithDefaults() *DeltaLakeTableV1`

NewDeltaLakeTableV1WithDefaults instantiates a new DeltaLakeTableV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *DeltaLakeTableV1) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *DeltaLakeTableV1) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *DeltaLakeTableV1) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *DeltaLakeTableV1) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocationHistory

`func (o *DeltaLakeTableV1) GetMetadataLocationHistory() []string`

GetMetadataLocationHistory returns the MetadataLocationHistory field if non-nil, zero value otherwise.

### GetMetadataLocationHistoryOk

`func (o *DeltaLakeTableV1) GetMetadataLocationHistoryOk() (*[]string, bool)`

GetMetadataLocationHistoryOk returns a tuple with the MetadataLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocationHistory

`func (o *DeltaLakeTableV1) SetMetadataLocationHistory(v []string)`

SetMetadataLocationHistory sets MetadataLocationHistory field to given value.


### GetCheckpointLocationHistory

`func (o *DeltaLakeTableV1) GetCheckpointLocationHistory() []string`

GetCheckpointLocationHistory returns the CheckpointLocationHistory field if non-nil, zero value otherwise.

### GetCheckpointLocationHistoryOk

`func (o *DeltaLakeTableV1) GetCheckpointLocationHistoryOk() (*[]string, bool)`

GetCheckpointLocationHistoryOk returns a tuple with the CheckpointLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCheckpointLocationHistory

`func (o *DeltaLakeTableV1) SetCheckpointLocationHistory(v []string)`

SetCheckpointLocationHistory sets CheckpointLocationHistory field to given value.


### GetLastCheckpoint

`func (o *DeltaLakeTableV1) GetLastCheckpoint() string`

GetLastCheckpoint returns the LastCheckpoint field if non-nil, zero value otherwise.

### GetLastCheckpointOk

`func (o *DeltaLakeTableV1) GetLastCheckpointOk() (*string, bool)`

GetLastCheckpointOk returns a tuple with the LastCheckpoint field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLastCheckpoint

`func (o *DeltaLakeTableV1) SetLastCheckpoint(v string)`

SetLastCheckpoint sets LastCheckpoint field to given value.

### HasLastCheckpoint

`func (o *DeltaLakeTableV1) HasLastCheckpoint() bool`

HasLastCheckpoint returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


