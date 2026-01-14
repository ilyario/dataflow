# MergeResponseV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ResultantTargetHash** | Pointer to **string** |  | [optional] 
**CommonAncestor** | Pointer to **string** |  | [optional] 
**TargetBranch** | Pointer to **string** |  | [optional] 
**EffectiveTargetHash** | Pointer to **string** |  | [optional] 
**ExpectedHash** | Pointer to **string** |  | [optional] 
**Details** | Pointer to [**[]ContentKeyDetailsV2**](ContentKeyDetailsV2.md) |  | [optional] 

## Methods

### NewMergeResponseV2

`func NewMergeResponseV2() *MergeResponseV2`

NewMergeResponseV2 instantiates a new MergeResponseV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewMergeResponseV2WithDefaults

`func NewMergeResponseV2WithDefaults() *MergeResponseV2`

NewMergeResponseV2WithDefaults instantiates a new MergeResponseV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetResultantTargetHash

`func (o *MergeResponseV2) GetResultantTargetHash() string`

GetResultantTargetHash returns the ResultantTargetHash field if non-nil, zero value otherwise.

### GetResultantTargetHashOk

`func (o *MergeResponseV2) GetResultantTargetHashOk() (*string, bool)`

GetResultantTargetHashOk returns a tuple with the ResultantTargetHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResultantTargetHash

`func (o *MergeResponseV2) SetResultantTargetHash(v string)`

SetResultantTargetHash sets ResultantTargetHash field to given value.

### HasResultantTargetHash

`func (o *MergeResponseV2) HasResultantTargetHash() bool`

HasResultantTargetHash returns a boolean if a field has been set.

### GetCommonAncestor

`func (o *MergeResponseV2) GetCommonAncestor() string`

GetCommonAncestor returns the CommonAncestor field if non-nil, zero value otherwise.

### GetCommonAncestorOk

`func (o *MergeResponseV2) GetCommonAncestorOk() (*string, bool)`

GetCommonAncestorOk returns a tuple with the CommonAncestor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommonAncestor

`func (o *MergeResponseV2) SetCommonAncestor(v string)`

SetCommonAncestor sets CommonAncestor field to given value.

### HasCommonAncestor

`func (o *MergeResponseV2) HasCommonAncestor() bool`

HasCommonAncestor returns a boolean if a field has been set.

### GetTargetBranch

`func (o *MergeResponseV2) GetTargetBranch() string`

GetTargetBranch returns the TargetBranch field if non-nil, zero value otherwise.

### GetTargetBranchOk

`func (o *MergeResponseV2) GetTargetBranchOk() (*string, bool)`

GetTargetBranchOk returns a tuple with the TargetBranch field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTargetBranch

`func (o *MergeResponseV2) SetTargetBranch(v string)`

SetTargetBranch sets TargetBranch field to given value.

### HasTargetBranch

`func (o *MergeResponseV2) HasTargetBranch() bool`

HasTargetBranch returns a boolean if a field has been set.

### GetEffectiveTargetHash

`func (o *MergeResponseV2) GetEffectiveTargetHash() string`

GetEffectiveTargetHash returns the EffectiveTargetHash field if non-nil, zero value otherwise.

### GetEffectiveTargetHashOk

`func (o *MergeResponseV2) GetEffectiveTargetHashOk() (*string, bool)`

GetEffectiveTargetHashOk returns a tuple with the EffectiveTargetHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveTargetHash

`func (o *MergeResponseV2) SetEffectiveTargetHash(v string)`

SetEffectiveTargetHash sets EffectiveTargetHash field to given value.

### HasEffectiveTargetHash

`func (o *MergeResponseV2) HasEffectiveTargetHash() bool`

HasEffectiveTargetHash returns a boolean if a field has been set.

### GetExpectedHash

`func (o *MergeResponseV2) GetExpectedHash() string`

GetExpectedHash returns the ExpectedHash field if non-nil, zero value otherwise.

### GetExpectedHashOk

`func (o *MergeResponseV2) GetExpectedHashOk() (*string, bool)`

GetExpectedHashOk returns a tuple with the ExpectedHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpectedHash

`func (o *MergeResponseV2) SetExpectedHash(v string)`

SetExpectedHash sets ExpectedHash field to given value.

### HasExpectedHash

`func (o *MergeResponseV2) HasExpectedHash() bool`

HasExpectedHash returns a boolean if a field has been set.

### GetDetails

`func (o *MergeResponseV2) GetDetails() []ContentKeyDetailsV2`

GetDetails returns the Details field if non-nil, zero value otherwise.

### GetDetailsOk

`func (o *MergeResponseV2) GetDetailsOk() (*[]ContentKeyDetailsV2, bool)`

GetDetailsOk returns a tuple with the Details field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDetails

`func (o *MergeResponseV2) SetDetails(v []ContentKeyDetailsV2)`

SetDetails sets Details field to given value.

### HasDetails

`func (o *MergeResponseV2) HasDetails() bool`

HasDetails returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


