# ContentV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**MetadataLocation** | **string** |  | 
**SnapshotId** | Pointer to **int64** |  | [optional] 
**SchemaId** | Pointer to **int32** |  | [optional] 
**SpecId** | Pointer to **int32** |  | [optional] 
**SortOrderId** | Pointer to **int32** |  | [optional] 
**MetadataLocationHistory** | **[]string** |  | 
**CheckpointLocationHistory** | **[]string** |  | 
**LastCheckpoint** | Pointer to **string** |  | [optional] 
**VersionId** | Pointer to **string** |  | [optional] 
**SqlText** | **string** |  | 
**Dialect** | Pointer to **string** |  | [optional] 
**Elements** | **[]string** |  | 
**Properties** | **map[string]string** |  | 
**SignatureId** | Pointer to **string** |  | [optional] 

## Methods

### NewContentV2

`func NewContentV2(metadataLocation string, metadataLocationHistory []string, checkpointLocationHistory []string, sqlText string, elements []string, properties map[string]string, ) *ContentV2`

NewContentV2 instantiates a new ContentV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewContentV2WithDefaults

`func NewContentV2WithDefaults() *ContentV2`

NewContentV2WithDefaults instantiates a new ContentV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *ContentV2) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *ContentV2) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *ContentV2) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *ContentV2) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocation

`func (o *ContentV2) GetMetadataLocation() string`

GetMetadataLocation returns the MetadataLocation field if non-nil, zero value otherwise.

### GetMetadataLocationOk

`func (o *ContentV2) GetMetadataLocationOk() (*string, bool)`

GetMetadataLocationOk returns a tuple with the MetadataLocation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocation

`func (o *ContentV2) SetMetadataLocation(v string)`

SetMetadataLocation sets MetadataLocation field to given value.


### GetSnapshotId

`func (o *ContentV2) GetSnapshotId() int64`

GetSnapshotId returns the SnapshotId field if non-nil, zero value otherwise.

### GetSnapshotIdOk

`func (o *ContentV2) GetSnapshotIdOk() (*int64, bool)`

GetSnapshotIdOk returns a tuple with the SnapshotId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSnapshotId

`func (o *ContentV2) SetSnapshotId(v int64)`

SetSnapshotId sets SnapshotId field to given value.

### HasSnapshotId

`func (o *ContentV2) HasSnapshotId() bool`

HasSnapshotId returns a boolean if a field has been set.

### GetSchemaId

`func (o *ContentV2) GetSchemaId() int32`

GetSchemaId returns the SchemaId field if non-nil, zero value otherwise.

### GetSchemaIdOk

`func (o *ContentV2) GetSchemaIdOk() (*int32, bool)`

GetSchemaIdOk returns a tuple with the SchemaId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSchemaId

`func (o *ContentV2) SetSchemaId(v int32)`

SetSchemaId sets SchemaId field to given value.

### HasSchemaId

`func (o *ContentV2) HasSchemaId() bool`

HasSchemaId returns a boolean if a field has been set.

### GetSpecId

`func (o *ContentV2) GetSpecId() int32`

GetSpecId returns the SpecId field if non-nil, zero value otherwise.

### GetSpecIdOk

`func (o *ContentV2) GetSpecIdOk() (*int32, bool)`

GetSpecIdOk returns a tuple with the SpecId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSpecId

`func (o *ContentV2) SetSpecId(v int32)`

SetSpecId sets SpecId field to given value.

### HasSpecId

`func (o *ContentV2) HasSpecId() bool`

HasSpecId returns a boolean if a field has been set.

### GetSortOrderId

`func (o *ContentV2) GetSortOrderId() int32`

GetSortOrderId returns the SortOrderId field if non-nil, zero value otherwise.

### GetSortOrderIdOk

`func (o *ContentV2) GetSortOrderIdOk() (*int32, bool)`

GetSortOrderIdOk returns a tuple with the SortOrderId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSortOrderId

`func (o *ContentV2) SetSortOrderId(v int32)`

SetSortOrderId sets SortOrderId field to given value.

### HasSortOrderId

`func (o *ContentV2) HasSortOrderId() bool`

HasSortOrderId returns a boolean if a field has been set.

### GetMetadataLocationHistory

`func (o *ContentV2) GetMetadataLocationHistory() []string`

GetMetadataLocationHistory returns the MetadataLocationHistory field if non-nil, zero value otherwise.

### GetMetadataLocationHistoryOk

`func (o *ContentV2) GetMetadataLocationHistoryOk() (*[]string, bool)`

GetMetadataLocationHistoryOk returns a tuple with the MetadataLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocationHistory

`func (o *ContentV2) SetMetadataLocationHistory(v []string)`

SetMetadataLocationHistory sets MetadataLocationHistory field to given value.


### GetCheckpointLocationHistory

`func (o *ContentV2) GetCheckpointLocationHistory() []string`

GetCheckpointLocationHistory returns the CheckpointLocationHistory field if non-nil, zero value otherwise.

### GetCheckpointLocationHistoryOk

`func (o *ContentV2) GetCheckpointLocationHistoryOk() (*[]string, bool)`

GetCheckpointLocationHistoryOk returns a tuple with the CheckpointLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCheckpointLocationHistory

`func (o *ContentV2) SetCheckpointLocationHistory(v []string)`

SetCheckpointLocationHistory sets CheckpointLocationHistory field to given value.


### GetLastCheckpoint

`func (o *ContentV2) GetLastCheckpoint() string`

GetLastCheckpoint returns the LastCheckpoint field if non-nil, zero value otherwise.

### GetLastCheckpointOk

`func (o *ContentV2) GetLastCheckpointOk() (*string, bool)`

GetLastCheckpointOk returns a tuple with the LastCheckpoint field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLastCheckpoint

`func (o *ContentV2) SetLastCheckpoint(v string)`

SetLastCheckpoint sets LastCheckpoint field to given value.

### HasLastCheckpoint

`func (o *ContentV2) HasLastCheckpoint() bool`

HasLastCheckpoint returns a boolean if a field has been set.

### GetVersionId

`func (o *ContentV2) GetVersionId() string`

GetVersionId returns the VersionId field if non-nil, zero value otherwise.

### GetVersionIdOk

`func (o *ContentV2) GetVersionIdOk() (*string, bool)`

GetVersionIdOk returns a tuple with the VersionId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersionId

`func (o *ContentV2) SetVersionId(v string)`

SetVersionId sets VersionId field to given value.

### HasVersionId

`func (o *ContentV2) HasVersionId() bool`

HasVersionId returns a boolean if a field has been set.

### GetSqlText

`func (o *ContentV2) GetSqlText() string`

GetSqlText returns the SqlText field if non-nil, zero value otherwise.

### GetSqlTextOk

`func (o *ContentV2) GetSqlTextOk() (*string, bool)`

GetSqlTextOk returns a tuple with the SqlText field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSqlText

`func (o *ContentV2) SetSqlText(v string)`

SetSqlText sets SqlText field to given value.


### GetDialect

`func (o *ContentV2) GetDialect() string`

GetDialect returns the Dialect field if non-nil, zero value otherwise.

### GetDialectOk

`func (o *ContentV2) GetDialectOk() (*string, bool)`

GetDialectOk returns a tuple with the Dialect field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDialect

`func (o *ContentV2) SetDialect(v string)`

SetDialect sets Dialect field to given value.

### HasDialect

`func (o *ContentV2) HasDialect() bool`

HasDialect returns a boolean if a field has been set.

### GetElements

`func (o *ContentV2) GetElements() []string`

GetElements returns the Elements field if non-nil, zero value otherwise.

### GetElementsOk

`func (o *ContentV2) GetElementsOk() (*[]string, bool)`

GetElementsOk returns a tuple with the Elements field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetElements

`func (o *ContentV2) SetElements(v []string)`

SetElements sets Elements field to given value.


### GetProperties

`func (o *ContentV2) GetProperties() map[string]string`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *ContentV2) GetPropertiesOk() (*map[string]string, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *ContentV2) SetProperties(v map[string]string)`

SetProperties sets Properties field to given value.


### GetSignatureId

`func (o *ContentV2) GetSignatureId() string`

GetSignatureId returns the SignatureId field if non-nil, zero value otherwise.

### GetSignatureIdOk

`func (o *ContentV2) GetSignatureIdOk() (*string, bool)`

GetSignatureIdOk returns a tuple with the SignatureId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSignatureId

`func (o *ContentV2) SetSignatureId(v string)`

SetSignatureId sets SignatureId field to given value.

### HasSignatureId

`func (o *ContentV2) HasSignatureId() bool`

HasSignatureId returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


