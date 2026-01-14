# Reference

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Hash** | Pointer to **string** |  | [optional] 
**Metadata** | Pointer to [**ReferenceMetadata**](ReferenceMetadata.md) |  | [optional] 

## Methods

### NewReference

`func NewReference(name string, ) *Reference`

NewReference instantiates a new Reference object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceWithDefaults

`func NewReferenceWithDefaults() *Reference`

NewReferenceWithDefaults instantiates a new Reference object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *Reference) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *Reference) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *Reference) SetName(v string)`

SetName sets Name field to given value.


### GetHash

`func (o *Reference) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *Reference) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *Reference) SetHash(v string)`

SetHash sets Hash field to given value.

### HasHash

`func (o *Reference) HasHash() bool`

HasHash returns a boolean if a field has been set.

### GetMetadata

`func (o *Reference) GetMetadata() ReferenceMetadata`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Reference) GetMetadataOk() (*ReferenceMetadata, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Reference) SetMetadata(v ReferenceMetadata)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *Reference) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


