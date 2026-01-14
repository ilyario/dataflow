# Detached

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Hash** | **string** |  | 
**Metadata** | Pointer to [**ReferenceMetadata**](ReferenceMetadata.md) |  | [optional] 

## Methods

### NewDetached

`func NewDetached(hash string, ) *Detached`

NewDetached instantiates a new Detached object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDetachedWithDefaults

`func NewDetachedWithDefaults() *Detached`

NewDetachedWithDefaults instantiates a new Detached object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHash

`func (o *Detached) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *Detached) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *Detached) SetHash(v string)`

SetHash sets Hash field to given value.


### GetMetadata

`func (o *Detached) GetMetadata() ReferenceMetadata`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Detached) GetMetadataOk() (*ReferenceMetadata, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Detached) SetMetadata(v ReferenceMetadata)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *Detached) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


