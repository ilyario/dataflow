# BranchV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Metadata** | Pointer to [**ReferenceMetadataV2**](ReferenceMetadataV2.md) |  | [optional] 
**Hash** | Pointer to **string** |  | [optional] 

## Methods

### NewBranchV2

`func NewBranchV2(name string, ) *BranchV2`

NewBranchV2 instantiates a new BranchV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewBranchV2WithDefaults

`func NewBranchV2WithDefaults() *BranchV2`

NewBranchV2WithDefaults instantiates a new BranchV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *BranchV2) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *BranchV2) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *BranchV2) SetName(v string)`

SetName sets Name field to given value.


### GetMetadata

`func (o *BranchV2) GetMetadata() ReferenceMetadataV2`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *BranchV2) GetMetadataOk() (*ReferenceMetadataV2, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *BranchV2) SetMetadata(v ReferenceMetadataV2)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *BranchV2) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetHash

`func (o *BranchV2) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *BranchV2) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *BranchV2) SetHash(v string)`

SetHash sets Hash field to given value.

### HasHash

`func (o *BranchV2) HasHash() bool`

HasHash returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


