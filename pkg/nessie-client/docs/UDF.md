# UDF

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | Pointer to **string** |  | [optional] 
**SqlText** | **string** |  | 
**Dialect** | Pointer to **string** |  | [optional] 
**VersionId** | Pointer to **string** |  | [optional] 
**SignatureId** | Pointer to **string** |  | [optional] 
**MetadataLocation** | Pointer to **string** |  | [optional] 

## Methods

### NewUDF

`func NewUDF(sqlText string, ) *UDF`

NewUDF instantiates a new UDF object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewUDFWithDefaults

`func NewUDFWithDefaults() *UDF`

NewUDFWithDefaults instantiates a new UDF object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *UDF) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *UDF) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *UDF) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *UDF) HasId() bool`

HasId returns a boolean if a field has been set.

### GetSqlText

`func (o *UDF) GetSqlText() string`

GetSqlText returns the SqlText field if non-nil, zero value otherwise.

### GetSqlTextOk

`func (o *UDF) GetSqlTextOk() (*string, bool)`

GetSqlTextOk returns a tuple with the SqlText field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSqlText

`func (o *UDF) SetSqlText(v string)`

SetSqlText sets SqlText field to given value.


### GetDialect

`func (o *UDF) GetDialect() string`

GetDialect returns the Dialect field if non-nil, zero value otherwise.

### GetDialectOk

`func (o *UDF) GetDialectOk() (*string, bool)`

GetDialectOk returns a tuple with the Dialect field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDialect

`func (o *UDF) SetDialect(v string)`

SetDialect sets Dialect field to given value.

### HasDialect

`func (o *UDF) HasDialect() bool`

HasDialect returns a boolean if a field has been set.

### GetVersionId

`func (o *UDF) GetVersionId() string`

GetVersionId returns the VersionId field if non-nil, zero value otherwise.

### GetVersionIdOk

`func (o *UDF) GetVersionIdOk() (*string, bool)`

GetVersionIdOk returns a tuple with the VersionId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersionId

`func (o *UDF) SetVersionId(v string)`

SetVersionId sets VersionId field to given value.

### HasVersionId

`func (o *UDF) HasVersionId() bool`

HasVersionId returns a boolean if a field has been set.

### GetSignatureId

`func (o *UDF) GetSignatureId() string`

GetSignatureId returns the SignatureId field if non-nil, zero value otherwise.

### GetSignatureIdOk

`func (o *UDF) GetSignatureIdOk() (*string, bool)`

GetSignatureIdOk returns a tuple with the SignatureId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSignatureId

`func (o *UDF) SetSignatureId(v string)`

SetSignatureId sets SignatureId field to given value.

### HasSignatureId

`func (o *UDF) HasSignatureId() bool`

HasSignatureId returns a boolean if a field has been set.

### GetMetadataLocation

`func (o *UDF) GetMetadataLocation() string`

GetMetadataLocation returns the MetadataLocation field if non-nil, zero value otherwise.

### GetMetadataLocationOk

`func (o *UDF) GetMetadataLocationOk() (*string, bool)`

GetMetadataLocationOk returns a tuple with the MetadataLocation field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadataLocation

`func (o *UDF) SetMetadataLocation(v string)`

SetMetadataLocation sets MetadataLocation field to given value.

### HasMetadataLocation

`func (o *UDF) HasMetadataLocation() bool`

HasMetadataLocation returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


