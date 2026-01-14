# DetachedV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Hash** | **string** |  | 
**Metadata** | Pointer to [**ReferenceMetadataV1**](ReferenceMetadataV1.md) |  | [optional] 

## Methods

### NewDetachedV1

`func NewDetachedV1(hash string, ) *DetachedV1`

NewDetachedV1 instantiates a new DetachedV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDetachedV1WithDefaults

`func NewDetachedV1WithDefaults() *DetachedV1`

NewDetachedV1WithDefaults instantiates a new DetachedV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHash

`func (o *DetachedV1) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *DetachedV1) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *DetachedV1) SetHash(v string)`

SetHash sets Hash field to given value.


### GetMetadata

`func (o *DetachedV1) GetMetadata() ReferenceMetadataV1`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *DetachedV1) GetMetadataOk() (*ReferenceMetadataV1, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *DetachedV1) SetMetadata(v ReferenceMetadataV1)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *DetachedV1) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


