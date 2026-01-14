# IcebergTableV1

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

### NewIcebergTableV1

`func NewIcebergTableV1(metadataLocation string, ) *IcebergTableV1`

NewIcebergTableV1 instantiates a new IcebergTableV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewIcebergTableV1WithDefaults

`func NewIcebergTableV1WithDefaults() *IcebergTableV1`

NewIcebergTableV1WithDefaults instantiates a new IcebergTableV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *IcebergTableV1) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *IcebergTableV1) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *IcebergTableV1) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *IcebergTableV1) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocation

`func (o *IcebergTableV1) GetMetadataLocation() string`

GetMetadataLocation returns the MetadataLocation field if non-nil, zero value otherwise.

### GetMetadataLocationOk

`func (o *IcebergTableV1) GetMetadataLocationOk() (*string, bool)`

GetMetadataLocationOk returns a tuple with the MetadataLocation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocation

`func (o *IcebergTableV1) SetMetadataLocation(v string)`

SetMetadataLocation sets MetadataLocation field to given value.


### GetSnapshotId

`func (o *IcebergTableV1) GetSnapshotId() int64`

GetSnapshotId returns the SnapshotId field if non-nil, zero value otherwise.

### GetSnapshotIdOk

`func (o *IcebergTableV1) GetSnapshotIdOk() (*int64, bool)`

GetSnapshotIdOk returns a tuple with the SnapshotId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSnapshotId

`func (o *IcebergTableV1) SetSnapshotId(v int64)`

SetSnapshotId sets SnapshotId field to given value.

### HasSnapshotId

`func (o *IcebergTableV1) HasSnapshotId() bool`

HasSnapshotId returns a boolean if a field has been set.

### GetSchemaId

`func (o *IcebergTableV1) GetSchemaId() int32`

GetSchemaId returns the SchemaId field if non-nil, zero value otherwise.

### GetSchemaIdOk

`func (o *IcebergTableV1) GetSchemaIdOk() (*int32, bool)`

GetSchemaIdOk returns a tuple with the SchemaId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSchemaId

`func (o *IcebergTableV1) SetSchemaId(v int32)`

SetSchemaId sets SchemaId field to given value.

### HasSchemaId

`func (o *IcebergTableV1) HasSchemaId() bool`

HasSchemaId returns a boolean if a field has been set.

### GetSpecId

`func (o *IcebergTableV1) GetSpecId() int32`

GetSpecId returns the SpecId field if non-nil, zero value otherwise.

### GetSpecIdOk

`func (o *IcebergTableV1) GetSpecIdOk() (*int32, bool)`

GetSpecIdOk returns a tuple with the SpecId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSpecId

`func (o *IcebergTableV1) SetSpecId(v int32)`

SetSpecId sets SpecId field to given value.

### HasSpecId

`func (o *IcebergTableV1) HasSpecId() bool`

HasSpecId returns a boolean if a field has been set.

### GetSortOrderId

`func (o *IcebergTableV1) GetSortOrderId() int32`

GetSortOrderId returns the SortOrderId field if non-nil, zero value otherwise.

### GetSortOrderIdOk

`func (o *IcebergTableV1) GetSortOrderIdOk() (*int32, bool)`

GetSortOrderIdOk returns a tuple with the SortOrderId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSortOrderId

`func (o *IcebergTableV1) SetSortOrderId(v int32)`

SetSortOrderId sets SortOrderId field to given value.

### HasSortOrderId

`func (o *IcebergTableV1) HasSortOrderId() bool`

HasSortOrderId returns a boolean if a field has been set.

### GetMetadata

`func (o *IcebergTableV1) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *IcebergTableV1) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *IcebergTableV1) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *IcebergTableV1) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


