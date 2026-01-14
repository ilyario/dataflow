# ContentV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**MetadataLocation** | **string** |  | 
**SnapshotId** | Pointer to **int64** |  | [optional] 
**SchemaId** | Pointer to **int32** |  | [optional] 
**SpecId** | Pointer to **int32** |  | [optional] 
**SortOrderId** | Pointer to **int32** |  | [optional] 
**Metadata** | Pointer to **map[string]interface{}** |  | [optional] 
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

### NewContentV1

`func NewContentV1(metadataLocation string, metadataLocationHistory []string, checkpointLocationHistory []string, sqlText string, elements []string, properties map[string]string, ) *ContentV1`

NewContentV1 instantiates a new ContentV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewContentV1WithDefaults

`func NewContentV1WithDefaults() *ContentV1`

NewContentV1WithDefaults instantiates a new ContentV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *ContentV1) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *ContentV1) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *ContentV1) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *ContentV1) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocation

`func (o *ContentV1) GetMetadataLocation() string`

GetMetadataLocation returns the MetadataLocation field if non-nil, zero value otherwise.

### GetMetadataLocationOk

`func (o *ContentV1) GetMetadataLocationOk() (*string, bool)`

GetMetadataLocationOk returns a tuple with the MetadataLocation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocation

`func (o *ContentV1) SetMetadataLocation(v string)`

SetMetadataLocation sets MetadataLocation field to given value.


### GetSnapshotId

`func (o *ContentV1) GetSnapshotId() int64`

GetSnapshotId returns the SnapshotId field if non-nil, zero value otherwise.

### GetSnapshotIdOk

`func (o *ContentV1) GetSnapshotIdOk() (*int64, bool)`

GetSnapshotIdOk returns a tuple with the SnapshotId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSnapshotId

`func (o *ContentV1) SetSnapshotId(v int64)`

SetSnapshotId sets SnapshotId field to given value.

### HasSnapshotId

`func (o *ContentV1) HasSnapshotId() bool`

HasSnapshotId returns a boolean if a field has been set.

### GetSchemaId

`func (o *ContentV1) GetSchemaId() int32`

GetSchemaId returns the SchemaId field if non-nil, zero value otherwise.

### GetSchemaIdOk

`func (o *ContentV1) GetSchemaIdOk() (*int32, bool)`

GetSchemaIdOk returns a tuple with the SchemaId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSchemaId

`func (o *ContentV1) SetSchemaId(v int32)`

SetSchemaId sets SchemaId field to given value.

### HasSchemaId

`func (o *ContentV1) HasSchemaId() bool`

HasSchemaId returns a boolean if a field has been set.

### GetSpecId

`func (o *ContentV1) GetSpecId() int32`

GetSpecId returns the SpecId field if non-nil, zero value otherwise.

### GetSpecIdOk

`func (o *ContentV1) GetSpecIdOk() (*int32, bool)`

GetSpecIdOk returns a tuple with the SpecId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSpecId

`func (o *ContentV1) SetSpecId(v int32)`

SetSpecId sets SpecId field to given value.

### HasSpecId

`func (o *ContentV1) HasSpecId() bool`

HasSpecId returns a boolean if a field has been set.

### GetSortOrderId

`func (o *ContentV1) GetSortOrderId() int32`

GetSortOrderId returns the SortOrderId field if non-nil, zero value otherwise.

### GetSortOrderIdOk

`func (o *ContentV1) GetSortOrderIdOk() (*int32, bool)`

GetSortOrderIdOk returns a tuple with the SortOrderId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSortOrderId

`func (o *ContentV1) SetSortOrderId(v int32)`

SetSortOrderId sets SortOrderId field to given value.

### HasSortOrderId

`func (o *ContentV1) HasSortOrderId() bool`

HasSortOrderId returns a boolean if a field has been set.

### GetMetadata

`func (o *ContentV1) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *ContentV1) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *ContentV1) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *ContentV1) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetMetadataLocationHistory

`func (o *ContentV1) GetMetadataLocationHistory() []string`

GetMetadataLocationHistory returns the MetadataLocationHistory field if non-nil, zero value otherwise.

### GetMetadataLocationHistoryOk

`func (o *ContentV1) GetMetadataLocationHistoryOk() (*[]string, bool)`

GetMetadataLocationHistoryOk returns a tuple with the MetadataLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocationHistory

`func (o *ContentV1) SetMetadataLocationHistory(v []string)`

SetMetadataLocationHistory sets MetadataLocationHistory field to given value.


### GetCheckpointLocationHistory

`func (o *ContentV1) GetCheckpointLocationHistory() []string`

GetCheckpointLocationHistory returns the CheckpointLocationHistory field if non-nil, zero value otherwise.

### GetCheckpointLocationHistoryOk

`func (o *ContentV1) GetCheckpointLocationHistoryOk() (*[]string, bool)`

GetCheckpointLocationHistoryOk returns a tuple with the CheckpointLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCheckpointLocationHistory

`func (o *ContentV1) SetCheckpointLocationHistory(v []string)`

SetCheckpointLocationHistory sets CheckpointLocationHistory field to given value.


### GetLastCheckpoint

`func (o *ContentV1) GetLastCheckpoint() string`

GetLastCheckpoint returns the LastCheckpoint field if non-nil, zero value otherwise.

### GetLastCheckpointOk

`func (o *ContentV1) GetLastCheckpointOk() (*string, bool)`

GetLastCheckpointOk returns a tuple with the LastCheckpoint field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLastCheckpoint

`func (o *ContentV1) SetLastCheckpoint(v string)`

SetLastCheckpoint sets LastCheckpoint field to given value.

### HasLastCheckpoint

`func (o *ContentV1) HasLastCheckpoint() bool`

HasLastCheckpoint returns a boolean if a field has been set.

### GetVersionId

`func (o *ContentV1) GetVersionId() string`

GetVersionId returns the VersionId field if non-nil, zero value otherwise.

### GetVersionIdOk

`func (o *ContentV1) GetVersionIdOk() (*string, bool)`

GetVersionIdOk returns a tuple with the VersionId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersionId

`func (o *ContentV1) SetVersionId(v string)`

SetVersionId sets VersionId field to given value.

### HasVersionId

`func (o *ContentV1) HasVersionId() bool`

HasVersionId returns a boolean if a field has been set.

### GetSqlText

`func (o *ContentV1) GetSqlText() string`

GetSqlText returns the SqlText field if non-nil, zero value otherwise.

### GetSqlTextOk

`func (o *ContentV1) GetSqlTextOk() (*string, bool)`

GetSqlTextOk returns a tuple with the SqlText field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSqlText

`func (o *ContentV1) SetSqlText(v string)`

SetSqlText sets SqlText field to given value.


### GetDialect

`func (o *ContentV1) GetDialect() string`

GetDialect returns the Dialect field if non-nil, zero value otherwise.

### GetDialectOk

`func (o *ContentV1) GetDialectOk() (*string, bool)`

GetDialectOk returns a tuple with the Dialect field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDialect

`func (o *ContentV1) SetDialect(v string)`

SetDialect sets Dialect field to given value.

### HasDialect

`func (o *ContentV1) HasDialect() bool`

HasDialect returns a boolean if a field has been set.

### GetElements

`func (o *ContentV1) GetElements() []string`

GetElements returns the Elements field if non-nil, zero value otherwise.

### GetElementsOk

`func (o *ContentV1) GetElementsOk() (*[]string, bool)`

GetElementsOk returns a tuple with the Elements field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetElements

`func (o *ContentV1) SetElements(v []string)`

SetElements sets Elements field to given value.


### GetProperties

`func (o *ContentV1) GetProperties() map[string]string`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *ContentV1) GetPropertiesOk() (*map[string]string, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *ContentV1) SetProperties(v map[string]string)`

SetProperties sets Properties field to given value.


### GetSignatureId

`func (o *ContentV1) GetSignatureId() string`

GetSignatureId returns the SignatureId field if non-nil, zero value otherwise.

### GetSignatureIdOk

`func (o *ContentV1) GetSignatureIdOk() (*string, bool)`

GetSignatureIdOk returns a tuple with the SignatureId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSignatureId

`func (o *ContentV1) SetSignatureId(v string)`

SetSignatureId sets SignatureId field to given value.

### HasSignatureId

`func (o *ContentV1) HasSignatureId() bool`

HasSignatureId returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


