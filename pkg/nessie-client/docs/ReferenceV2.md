# ReferenceV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Hash** | Pointer to **string** |  | [optional] 
**Metadata** | Pointer to [**ReferenceMetadataV2**](ReferenceMetadataV2.md) |  | [optional] 

## Methods

### NewReferenceV2

`func NewReferenceV2(name string, ) *ReferenceV2`

NewReferenceV2 instantiates a new ReferenceV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceV2WithDefaults

`func NewReferenceV2WithDefaults() *ReferenceV2`

NewReferenceV2WithDefaults instantiates a new ReferenceV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *ReferenceV2) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ReferenceV2) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ReferenceV2) SetName(v string)`

SetName sets Name field to given value.


### GetHash

`func (o *ReferenceV2) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *ReferenceV2) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *ReferenceV2) SetHash(v string)`

SetHash sets Hash field to given value.

### HasHash

`func (o *ReferenceV2) HasHash() bool`

HasHash returns a boolean if a field has been set.

### GetMetadata

`func (o *ReferenceV2) GetMetadata() ReferenceMetadataV2`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *ReferenceV2) GetMetadataOk() (*ReferenceMetadataV2, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *ReferenceV2) SetMetadata(v ReferenceMetadataV2)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *ReferenceV2) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


