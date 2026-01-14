# MergeResponseV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ResultantTargetHash** | Pointer to **string** |  | [optional] 
**CommonAncestor** | Pointer to **string** |  | [optional] 
**TargetBranch** | Pointer to **string** |  | [optional] 
**EffectiveTargetHash** | Pointer to **string** |  | [optional] 
**ExpectedHash** | Pointer to **string** |  | [optional] 
**Details** | Pointer to [**[]ContentKeyDetailsV1**](ContentKeyDetailsV1.md) |  | [optional] 

## Methods

### NewMergeResponseV1

`func NewMergeResponseV1() *MergeResponseV1`

NewMergeResponseV1 instantiates a new MergeResponseV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewMergeResponseV1WithDefaults

`func NewMergeResponseV1WithDefaults() *MergeResponseV1`

NewMergeResponseV1WithDefaults instantiates a new MergeResponseV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetResultantTargetHash

`func (o *MergeResponseV1) GetResultantTargetHash() string`

GetResultantTargetHash returns the ResultantTargetHash field if non-nil, zero value otherwise.

### GetResultantTargetHashOk

`func (o *MergeResponseV1) GetResultantTargetHashOk() (*string, bool)`

GetResultantTargetHashOk returns a tuple with the ResultantTargetHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResultantTargetHash

`func (o *MergeResponseV1) SetResultantTargetHash(v string)`

SetResultantTargetHash sets ResultantTargetHash field to given value.

### HasResultantTargetHash

`func (o *MergeResponseV1) HasResultantTargetHash() bool`

HasResultantTargetHash returns a boolean if a field has been set.

### GetCommonAncestor

`func (o *MergeResponseV1) GetCommonAncestor() string`

GetCommonAncestor returns the CommonAncestor field if non-nil, zero value otherwise.

### GetCommonAncestorOk

`func (o *MergeResponseV1) GetCommonAncestorOk() (*string, bool)`

GetCommonAncestorOk returns a tuple with the CommonAncestor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommonAncestor

`func (o *MergeResponseV1) SetCommonAncestor(v string)`

SetCommonAncestor sets CommonAncestor field to given value.

### HasCommonAncestor

`func (o *MergeResponseV1) HasCommonAncestor() bool`

HasCommonAncestor returns a boolean if a field has been set.

### GetTargetBranch

`func (o *MergeResponseV1) GetTargetBranch() string`

GetTargetBranch returns the TargetBranch field if non-nil, zero value otherwise.

### GetTargetBranchOk

`func (o *MergeResponseV1) GetTargetBranchOk() (*string, bool)`

GetTargetBranchOk returns a tuple with the TargetBranch field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTargetBranch

`func (o *MergeResponseV1) SetTargetBranch(v string)`

SetTargetBranch sets TargetBranch field to given value.

### HasTargetBranch

`func (o *MergeResponseV1) HasTargetBranch() bool`

HasTargetBranch returns a boolean if a field has been set.

### GetEffectiveTargetHash

`func (o *MergeResponseV1) GetEffectiveTargetHash() string`

GetEffectiveTargetHash returns the EffectiveTargetHash field if non-nil, zero value otherwise.

### GetEffectiveTargetHashOk

`func (o *MergeResponseV1) GetEffectiveTargetHashOk() (*string, bool)`

GetEffectiveTargetHashOk returns a tuple with the EffectiveTargetHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveTargetHash

`func (o *MergeResponseV1) SetEffectiveTargetHash(v string)`

SetEffectiveTargetHash sets EffectiveTargetHash field to given value.

### HasEffectiveTargetHash

`func (o *MergeResponseV1) HasEffectiveTargetHash() bool`

HasEffectiveTargetHash returns a boolean if a field has been set.

### GetExpectedHash

`func (o *MergeResponseV1) GetExpectedHash() string`

GetExpectedHash returns the ExpectedHash field if non-nil, zero value otherwise.

### GetExpectedHashOk

`func (o *MergeResponseV1) GetExpectedHashOk() (*string, bool)`

GetExpectedHashOk returns a tuple with the ExpectedHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpectedHash

`func (o *MergeResponseV1) SetExpectedHash(v string)`

SetExpectedHash sets ExpectedHash field to given value.

### HasExpectedHash

`func (o *MergeResponseV1) HasExpectedHash() bool`

HasExpectedHash returns a boolean if a field has been set.

### GetDetails

`func (o *MergeResponseV1) GetDetails() []ContentKeyDetailsV1`

GetDetails returns the Details field if non-nil, zero value otherwise.

### GetDetailsOk

`func (o *MergeResponseV1) GetDetailsOk() (*[]ContentKeyDetailsV1, bool)`

GetDetailsOk returns a tuple with the Details field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDetails

`func (o *MergeResponseV1) SetDetails(v []ContentKeyDetailsV1)`

SetDetails sets Details field to given value.

### HasDetails

`func (o *MergeResponseV1) HasDetails() bool`

HasDetails returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


