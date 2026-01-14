# IcebergTable

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

## Methods

### NewIcebergTable

`func NewIcebergTable(metadataLocation string, ) *IcebergTable`

NewIcebergTable instantiates a new IcebergTable object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewIcebergTableWithDefaults

`func NewIcebergTableWithDefaults() *IcebergTable`

NewIcebergTableWithDefaults instantiates a new IcebergTable object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *IcebergTable) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *IcebergTable) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *IcebergTable) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *IcebergTable) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocation

`func (o *IcebergTable) GetMetadataLocation() string`

GetMetadataLocation returns the MetadataLocation field if non-nil, zero value otherwise.

### GetMetadataLocationOk

`func (o *IcebergTable) GetMetadataLocationOk() (*string, bool)`

GetMetadataLocationOk returns a tuple with the MetadataLocation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocation

`func (o *IcebergTable) SetMetadataLocation(v string)`

SetMetadataLocation sets MetadataLocation field to given value.


### GetSnapshotId

`func (o *IcebergTable) GetSnapshotId() int64`

GetSnapshotId returns the SnapshotId field if non-nil, zero value otherwise.

### GetSnapshotIdOk

`func (o *IcebergTable) GetSnapshotIdOk() (*int64, bool)`

GetSnapshotIdOk returns a tuple with the SnapshotId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSnapshotId

`func (o *IcebergTable) SetSnapshotId(v int64)`

SetSnapshotId sets SnapshotId field to given value.

### HasSnapshotId

`func (o *IcebergTable) HasSnapshotId() bool`

HasSnapshotId returns a boolean if a field has been set.

### GetSchemaId

`func (o *IcebergTable) GetSchemaId() int32`

GetSchemaId returns the SchemaId field if non-nil, zero value otherwise.

### GetSchemaIdOk

`func (o *IcebergTable) GetSchemaIdOk() (*int32, bool)`

GetSchemaIdOk returns a tuple with the SchemaId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSchemaId

`func (o *IcebergTable) SetSchemaId(v int32)`

SetSchemaId sets SchemaId field to given value.

### HasSchemaId

`func (o *IcebergTable) HasSchemaId() bool`

HasSchemaId returns a boolean if a field has been set.

### GetSpecId

`func (o *IcebergTable) GetSpecId() int32`

GetSpecId returns the SpecId field if non-nil, zero value otherwise.

### GetSpecIdOk

`func (o *IcebergTable) GetSpecIdOk() (*int32, bool)`

GetSpecIdOk returns a tuple with the SpecId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSpecId

`func (o *IcebergTable) SetSpecId(v int32)`

SetSpecId sets SpecId field to given value.

### HasSpecId

`func (o *IcebergTable) HasSpecId() bool`

HasSpecId returns a boolean if a field has been set.

### GetSortOrderId

`func (o *IcebergTable) GetSortOrderId() int32`

GetSortOrderId returns the SortOrderId field if non-nil, zero value otherwise.

### GetSortOrderIdOk

`func (o *IcebergTable) GetSortOrderIdOk() (*int32, bool)`

GetSortOrderIdOk returns a tuple with the SortOrderId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSortOrderId

`func (o *IcebergTable) SetSortOrderId(v int32)`

SetSortOrderId sets SortOrderId field to given value.

### HasSortOrderId

`func (o *IcebergTable) HasSortOrderId() bool`

HasSortOrderId returns a boolean if a field has been set.

### GetMetadata

`func (o *IcebergTable) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *IcebergTable) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *IcebergTable) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *IcebergTable) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


