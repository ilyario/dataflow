# IcebergViewV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**MetadataLocation** | **string** |  | 
**VersionId** | Pointer to **int64** |  | [optional] 
**SchemaId** | Pointer to **int32** |  | [optional] 
**SqlText** | **string** |  | 
**Dialect** | Pointer to **string** |  | [optional] 

## Methods

### NewIcebergViewV2

`func NewIcebergViewV2(metadataLocation string, sqlText string, ) *IcebergViewV2`

NewIcebergViewV2 instantiates a new IcebergViewV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewIcebergViewV2WithDefaults

`func NewIcebergViewV2WithDefaults() *IcebergViewV2`

NewIcebergViewV2WithDefaults instantiates a new IcebergViewV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *IcebergViewV2) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *IcebergViewV2) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *IcebergViewV2) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *IcebergViewV2) HasId() bool`

HasId returns a boolean if a field has been set.

### GetMetadataLocation

`func (o *IcebergViewV2) GetMetadataLocation() string`

GetMetadataLocation returns the MetadataLocation field if non-nil, zero value otherwise.

### GetMetadataLocationOk

`func (o *IcebergViewV2) GetMetadataLocationOk() (*string, bool)`

GetMetadataLocationOk returns a tuple with the MetadataLocation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocation

`func (o *IcebergViewV2) SetMetadataLocation(v string)`

SetMetadataLocation sets MetadataLocation field to given value.


### GetVersionId

`func (o *IcebergViewV2) GetVersionId() int64`

GetVersionId returns the VersionId field if non-nil, zero value otherwise.

### GetVersionIdOk

`func (o *IcebergViewV2) GetVersionIdOk() (*int64, bool)`

GetVersionIdOk returns a tuple with the VersionId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersionId

`func (o *IcebergViewV2) SetVersionId(v int64)`

SetVersionId sets VersionId field to given value.

### HasVersionId

`func (o *IcebergViewV2) HasVersionId() bool`

HasVersionId returns a boolean if a field has been set.

### GetSchemaId

`func (o *IcebergViewV2) GetSchemaId() int32`

GetSchemaId returns the SchemaId field if non-nil, zero value otherwise.

### GetSchemaIdOk

`func (o *IcebergViewV2) GetSchemaIdOk() (*int32, bool)`

GetSchemaIdOk returns a tuple with the SchemaId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSchemaId

`func (o *IcebergViewV2) SetSchemaId(v int32)`

SetSchemaId sets SchemaId field to given value.

### HasSchemaId

`func (o *IcebergViewV2) HasSchemaId() bool`

HasSchemaId returns a boolean if a field has been set.

### GetSqlText

`func (o *IcebergViewV2) GetSqlText() string`

GetSqlText returns the SqlText field if non-nil, zero value otherwise.

### GetSqlTextOk

`func (o *IcebergViewV2) GetSqlTextOk() (*string, bool)`

GetSqlTextOk returns a tuple with the SqlText field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSqlText

`func (o *IcebergViewV2) SetSqlText(v string)`

SetSqlText sets SqlText field to given value.


### GetDialect

`func (o *IcebergViewV2) GetDialect() string`

GetDialect returns the Dialect field if non-nil, zero value otherwise.

### GetDialectOk

`func (o *IcebergViewV2) GetDialectOk() (*string, bool)`

GetDialectOk returns a tuple with the Dialect field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDialect

`func (o *IcebergViewV2) SetDialect(v string)`

SetDialect sets Dialect field to given value.

### HasDialect

`func (o *IcebergViewV2) HasDialect() bool`

HasDialect returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


