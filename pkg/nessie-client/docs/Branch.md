# Branch

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Metadata** | Pointer to [**ReferenceMetadata**](ReferenceMetadata.md) |  | [optional] 
**Hash** | Pointer to **string** |  | [optional] 

## Methods

### NewBranch

`func NewBranch(name string, ) *Branch`

NewBranch instantiates a new Branch object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewBranchWithDefaults

`func NewBranchWithDefaults() *Branch`

NewBranchWithDefaults instantiates a new Branch object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *Branch) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *Branch) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *Branch) SetName(v string)`

SetName sets Name field to given value.


### GetMetadata

`func (o *Branch) GetMetadata() ReferenceMetadata`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Branch) GetMetadataOk() (*ReferenceMetadata, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Branch) SetMetadata(v ReferenceMetadata)`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *Branch) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetHash

`func (o *Branch) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *Branch) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *Branch) SetHash(v string)`

SetHash sets Hash field to given value.

### HasHash

`func (o *Branch) HasHash() bool`

HasHash returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


