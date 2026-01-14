# ReferenceV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Hash** | Pointer to **string** |  | [optional] 
**Metadata** | Pointer to [**ReferenceMetadataV1**](ReferenceMetadataV1.md) |  | [optional] 

## Methods

### NewReferenceV1

`func NewReferenceV1(name string, ) *ReferenceV1`

NewReferenceV1 instantiates a new ReferenceV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceV1WithDefaults

`func NewReferenceV1WithDefaults() *ReferenceV1`

NewReferenceV1WithDefaults instantiates a new ReferenceV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *ReferenceV1) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ReferenceV1) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ReferenceV1) SetName(v string)`

SetName sets Name field to given value.


### GetHash

`func (o *ReferenceV1) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *ReferenceV1) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *ReferenceV1) SetHash(v string)`

SetHash sets Hash field to given value.

### HasHash

`func (o *ReferenceV1) HasHash() bool`

HasHash returns a boolean if a field has been set.

### GetMetadata

`func (o *ReferenceV1) GetMetadata() ReferenceMetadataV1`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *ReferenceV1) GetMetadataOk() (*ReferenceMetadataV1, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *ReferenceV1) SetMetadata(v ReferenceMetadataV1)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *ReferenceV1) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


