# Content

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

### NewContent

`func NewContent(metadataLocation string, metadataLocationHistory []string, checkpointLocationHistory []string, sqlText string, elements []string, properties map[string]string, ) *Content`

NewContent instantiates a new Content object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewContentWithDefaults

`func NewContentWithDefaults() *Content`

NewContentWithDefaults instantiates a new Content object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *Content) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *Content) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *Content) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *Content) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocation

`func (o *Content) GetMetadataLocation() string`

GetMetadataLocation returns the MetadataLocation field if non-nil, zero value otherwise.

### GetMetadataLocationOk

`func (o *Content) GetMetadataLocationOk() (*string, bool)`

GetMetadataLocationOk returns a tuple with the MetadataLocation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocation

`func (o *Content) SetMetadataLocation(v string)`

SetMetadataLocation sets MetadataLocation field to given value.


### GetSnapshotId

`func (o *Content) GetSnapshotId() int64`

GetSnapshotId returns the SnapshotId field if non-nil, zero value otherwise.

### GetSnapshotIdOk

`func (o *Content) GetSnapshotIdOk() (*int64, bool)`

GetSnapshotIdOk returns a tuple with the SnapshotId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSnapshotId

`func (o *Content) SetSnapshotId(v int64)`

SetSnapshotId sets SnapshotId field to given value.

### HasSnapshotId

`func (o *Content) HasSnapshotId() bool`

HasSnapshotId returns a boolean if a field has been set.

### GetSchemaId

`func (o *Content) GetSchemaId() int32`

GetSchemaId returns the SchemaId field if non-nil, zero value otherwise.

### GetSchemaIdOk

`func (o *Content) GetSchemaIdOk() (*int32, bool)`

GetSchemaIdOk returns a tuple with the SchemaId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSchemaId

`func (o *Content) SetSchemaId(v int32)`

SetSchemaId sets SchemaId field to given value.

### HasSchemaId

`func (o *Content) HasSchemaId() bool`

HasSchemaId returns a boolean if a field has been set.

### GetSpecId

`func (o *Content) GetSpecId() int32`

GetSpecId returns the SpecId field if non-nil, zero value otherwise.

### GetSpecIdOk

`func (o *Content) GetSpecIdOk() (*int32, bool)`

GetSpecIdOk returns a tuple with the SpecId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSpecId

`func (o *Content) SetSpecId(v int32)`

SetSpecId sets SpecId field to given value.

### HasSpecId

`func (o *Content) HasSpecId() bool`

HasSpecId returns a boolean if a field has been set.

### GetSortOrderId

`func (o *Content) GetSortOrderId() int32`

GetSortOrderId returns the SortOrderId field if non-nil, zero value otherwise.

### GetSortOrderIdOk

`func (o *Content) GetSortOrderIdOk() (*int32, bool)`

GetSortOrderIdOk returns a tuple with the SortOrderId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSortOrderId

`func (o *Content) SetSortOrderId(v int32)`

SetSortOrderId sets SortOrderId field to given value.

### HasSortOrderId

`func (o *Content) HasSortOrderId() bool`

HasSortOrderId returns a boolean if a field has been set.

### GetMetadata

`func (o *Content) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Content) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Content) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *Content) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetMetadataLocationHistory

`func (o *Content) GetMetadataLocationHistory() []string`

GetMetadataLocationHistory returns the MetadataLocationHistory field if non-nil, zero value otherwise.

### GetMetadataLocationHistoryOk

`func (o *Content) GetMetadataLocationHistoryOk() (*[]string, bool)`

GetMetadataLocationHistoryOk returns a tuple with the MetadataLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocationHistory

`func (o *Content) SetMetadataLocationHistory(v []string)`

SetMetadataLocationHistory sets MetadataLocationHistory field to given value.


### GetCheckpointLocationHistory

`func (o *Content) GetCheckpointLocationHistory() []string`

GetCheckpointLocationHistory returns the CheckpointLocationHistory field if non-nil, zero value otherwise.

### GetCheckpointLocationHistoryOk

`func (o *Content) GetCheckpointLocationHistoryOk() (*[]string, bool)`

GetCheckpointLocationHistoryOk returns a tuple with the CheckpointLocationHistory field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCheckpointLocationHistory

`func (o *Content) SetCheckpointLocationHistory(v []string)`

SetCheckpointLocationHistory sets CheckpointLocationHistory field to given value.


### GetLastCheckpoint

`func (o *Content) GetLastCheckpoint() string`

GetLastCheckpoint returns the LastCheckpoint field if non-nil, zero value otherwise.

### GetLastCheckpointOk

`func (o *Content) GetLastCheckpointOk() (*string, bool)`

GetLastCheckpointOk returns a tuple with the LastCheckpoint field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLastCheckpoint

`func (o *Content) SetLastCheckpoint(v string)`

SetLastCheckpoint sets LastCheckpoint field to given value.

### HasLastCheckpoint

`func (o *Content) HasLastCheckpoint() bool`

HasLastCheckpoint returns a boolean if a field has been set.

### GetVersionId

`func (o *Content) GetVersionId() string`

GetVersionId returns the VersionId field if non-nil, zero value otherwise.

### GetVersionIdOk

`func (o *Content) GetVersionIdOk() (*string, bool)`

GetVersionIdOk returns a tuple with the VersionId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersionId

`func (o *Content) SetVersionId(v string)`

SetVersionId sets VersionId field to given value.

### HasVersionId

`func (o *Content) HasVersionId() bool`

HasVersionId returns a boolean if a field has been set.

### GetSqlText

`func (o *Content) GetSqlText() string`

GetSqlText returns the SqlText field if non-nil, zero value otherwise.

### GetSqlTextOk

`func (o *Content) GetSqlTextOk() (*string, bool)`

GetSqlTextOk returns a tuple with the SqlText field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSqlText

`func (o *Content) SetSqlText(v string)`

SetSqlText sets SqlText field to given value.


### GetDialect

`func (o *Content) GetDialect() string`

GetDialect returns the Dialect field if non-nil, zero value otherwise.

### GetDialectOk

`func (o *Content) GetDialectOk() (*string, bool)`

GetDialectOk returns a tuple with the Dialect field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDialect

`func (o *Content) SetDialect(v string)`

SetDialect sets Dialect field to given value.

### HasDialect

`func (o *Content) HasDialect() bool`

HasDialect returns a boolean if a field has been set.

### GetElements

`func (o *Content) GetElements() []string`

GetElements returns the Elements field if non-nil, zero value otherwise.

### GetElementsOk

`func (o *Content) GetElementsOk() (*[]string, bool)`

GetElementsOk returns a tuple with the Elements field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetElements

`func (o *Content) SetElements(v []string)`

SetElements sets Elements field to given value.


### GetProperties

`func (o *Content) GetProperties() map[string]string`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *Content) GetPropertiesOk() (*map[string]string, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *Content) SetProperties(v map[string]string)`

SetProperties sets Properties field to given value.


### GetSignatureId

`func (o *Content) GetSignatureId() string`

GetSignatureId returns the SignatureId field if non-nil, zero value otherwise.

### GetSignatureIdOk

`func (o *Content) GetSignatureIdOk() (*string, bool)`

GetSignatureIdOk returns a tuple with the SignatureId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSignatureId

`func (o *Content) SetSignatureId(v string)`

SetSignatureId sets SignatureId field to given value.

### HasSignatureId

`func (o *Content) HasSignatureId() bool`

HasSignatureId returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


