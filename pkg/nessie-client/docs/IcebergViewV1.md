# IcebergViewV1

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

### NewIcebergViewV1

`func NewIcebergViewV1(metadataLocation string, sqlText string, ) *IcebergViewV1`

NewIcebergViewV1 instantiates a new IcebergViewV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewIcebergViewV1WithDefaults

`func NewIcebergViewV1WithDefaults() *IcebergViewV1`

NewIcebergViewV1WithDefaults instantiates a new IcebergViewV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *IcebergViewV1) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *IcebergViewV1) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *IcebergViewV1) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *IcebergViewV1) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocation

`func (o *IcebergViewV1) GetMetadataLocation() string`

GetMetadataLocation returns the MetadataLocation field if non-nil, zero value otherwise.

### GetMetadataLocationOk

`func (o *IcebergViewV1) GetMetadataLocationOk() (*string, bool)`

GetMetadataLocationOk returns a tuple with the MetadataLocation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocation

`func (o *IcebergViewV1) SetMetadataLocation(v string)`

SetMetadataLocation sets MetadataLocation field to given value.


### GetVersionId

`func (o *IcebergViewV1) GetVersionId() int64`

GetVersionId returns the VersionId field if non-nil, zero value otherwise.

### GetVersionIdOk

`func (o *IcebergViewV1) GetVersionIdOk() (*int64, bool)`

GetVersionIdOk returns a tuple with the VersionId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersionId

`func (o *IcebergViewV1) SetVersionId(v int64)`

SetVersionId sets VersionId field to given value.

### HasVersionId

`func (o *IcebergViewV1) HasVersionId() bool`

HasVersionId returns a boolean if a field has been set.

### GetSchemaId

`func (o *IcebergViewV1) GetSchemaId() int32`

GetSchemaId returns the SchemaId field if non-nil, zero value otherwise.

### GetSchemaIdOk

`func (o *IcebergViewV1) GetSchemaIdOk() (*int32, bool)`

GetSchemaIdOk returns a tuple with the SchemaId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSchemaId

`func (o *IcebergViewV1) SetSchemaId(v int32)`

SetSchemaId sets SchemaId field to given value.

### HasSchemaId

`func (o *IcebergViewV1) HasSchemaId() bool`

HasSchemaId returns a boolean if a field has been set.

### GetSqlText

`func (o *IcebergViewV1) GetSqlText() string`

GetSqlText returns the SqlText field if non-nil, zero value otherwise.

### GetSqlTextOk

`func (o *IcebergViewV1) GetSqlTextOk() (*string, bool)`

GetSqlTextOk returns a tuple with the SqlText field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSqlText

`func (o *IcebergViewV1) SetSqlText(v string)`

SetSqlText sets SqlText field to given value.


### GetDialect

`func (o *IcebergViewV1) GetDialect() string`

GetDialect returns the Dialect field if non-nil, zero value otherwise.

### GetDialectOk

`func (o *IcebergViewV1) GetDialectOk() (*string, bool)`

GetDialectOk returns a tuple with the Dialect field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDialect

`func (o *IcebergViewV1) SetDialect(v string)`

SetDialect sets Dialect field to given value.

### HasDialect

`func (o *IcebergViewV1) HasDialect() bool`

HasDialect returns a boolean if a field has been set.

### GetMetadata

`func (o *IcebergViewV1) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *IcebergViewV1) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *IcebergViewV1) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *IcebergViewV1) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


