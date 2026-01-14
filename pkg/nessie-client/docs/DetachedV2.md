# DetachedV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Hash** | **string** |  | 
**Metadata** | Pointer to [**ReferenceMetadataV2**](ReferenceMetadataV2.md) |  | [optional] 

## Methods

### NewDetachedV2

`func NewDetachedV2(hash string, ) *DetachedV2`

NewDetachedV2 instantiates a new DetachedV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDetachedV2WithDefaults

`func NewDetachedV2WithDefaults() *DetachedV2`

NewDetachedV2WithDefaults instantiates a new DetachedV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHash

`func (o *DetachedV2) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *DetachedV2) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *DetachedV2) SetHash(v string)`

SetHash sets Hash field to given value.


### GetMetadata

`func (o *DetachedV2) GetMetadata() ReferenceMetadataV2`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *DetachedV2) GetMetadataOk() (*ReferenceMetadataV2, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *DetachedV2) SetMetadata(v ReferenceMetadataV2)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *DetachedV2) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


