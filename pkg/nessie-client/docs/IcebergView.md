# IcebergView

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**MetadataLocation** | **string** |  | 
**VersionId** | Pointer to **int64** |  | [optional] 
**SchemaId** | Pointer to **int32** |  | [optional] 
**SqlText** | **string** |  | 
**Dialect** | Pointer to **string** |  | [optional] 
**Metadata** | Pointer to **map[string]interface{}** |  | [optional] 

## Methods

### NewIcebergView

`func NewIcebergView(metadataLocation string, sqlText string, ) *IcebergView`

NewIcebergView instantiates a new IcebergView object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewIcebergViewWithDefaults

`func NewIcebergViewWithDefaults() *IcebergView`

NewIcebergViewWithDefaults instantiates a new IcebergView object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *IcebergView) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *IcebergView) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *IcebergView) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *IcebergView) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocation

`func (o *IcebergView) GetMetadataLocation() string`

GetMetadataLocation returns the MetadataLocation field if non-nil, zero value otherwise.

### GetMetadataLocationOk

`func (o *IcebergView) GetMetadataLocationOk() (*string, bool)`

GetMetadataLocationOk returns a tuple with the MetadataLocation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocation

`func (o *IcebergView) SetMetadataLocation(v string)`

SetMetadataLocation sets MetadataLocation field to given value.


### GetVersionId

`func (o *IcebergView) GetVersionId() int64`

GetVersionId returns the VersionId field if non-nil, zero value otherwise.

### GetVersionIdOk

`func (o *IcebergView) GetVersionIdOk() (*int64, bool)`

GetVersionIdOk returns a tuple with the VersionId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersionId

`func (o *IcebergView) SetVersionId(v int64)`

SetVersionId sets VersionId field to given value.

### HasVersionId

`func (o *IcebergView) HasVersionId() bool`

HasVersionId returns a boolean if a field has been set.

### GetSchemaId

`func (o *IcebergView) GetSchemaId() int32`

GetSchemaId returns the SchemaId field if non-nil, zero value otherwise.

### GetSchemaIdOk

`func (o *IcebergView) GetSchemaIdOk() (*int32, bool)`

GetSchemaIdOk returns a tuple with the SchemaId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSchemaId

`func (o *IcebergView) SetSchemaId(v int32)`

SetSchemaId sets SchemaId field to given value.

### HasSchemaId

`func (o *IcebergView) HasSchemaId() bool`

HasSchemaId returns a boolean if a field has been set.

### GetSqlText

`func (o *IcebergView) GetSqlText() string`

GetSqlText returns the SqlText field if non-nil, zero value otherwise.

### GetSqlTextOk

`func (o *IcebergView) GetSqlTextOk() (*string, bool)`

GetSqlTextOk returns a tuple with the SqlText field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSqlText

`func (o *IcebergView) SetSqlText(v string)`

SetSqlText sets SqlText field to given value.


### GetDialect

`func (o *IcebergView) GetDialect() string`

GetDialect returns the Dialect field if non-nil, zero value otherwise.

### GetDialectOk

`func (o *IcebergView) GetDialectOk() (*string, bool)`

GetDialectOk returns a tuple with the Dialect field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDialect

`func (o *IcebergView) SetDialect(v string)`

SetDialect sets Dialect field to given value.

### HasDialect

`func (o *IcebergView) HasDialect() bool`

HasDialect returns a boolean if a field has been set.

### GetMetadata

`func (o *IcebergView) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *IcebergView) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *IcebergView) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *IcebergView) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


