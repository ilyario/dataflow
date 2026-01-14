# MergeResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ResultantTargetHash** | Pointer to **string** |  | [optional] 
**CommonAncestor** | Pointer to **string** |  | [optional] 
**TargetBranch** | Pointer to **string** |  | [optional] 
**EffectiveTargetHash** | Pointer to **string** |  | [optional] 
**ExpectedHash** | Pointer to **string** |  | [optional] 
**Details** | Pointer to [**[]ContentKeyDetails**](ContentKeyDetails.md) |  | [optional] 

## Methods

### NewMergeResponse

`func NewMergeResponse() *MergeResponse`

NewMergeResponse instantiates a new MergeResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewMergeResponseWithDefaults

`func NewMergeResponseWithDefaults() *MergeResponse`

NewMergeResponseWithDefaults instantiates a new MergeResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetResultantTargetHash

`func (o *MergeResponse) GetResultantTargetHash() string`

GetResultantTargetHash returns the ResultantTargetHash field if non-nil, zero value otherwise.

### GetResultantTargetHashOk

`func (o *MergeResponse) GetResultantTargetHashOk() (*string, bool)`

GetResultantTargetHashOk returns a tuple with the ResultantTargetHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResultantTargetHash

`func (o *MergeResponse) SetResultantTargetHash(v string)`

SetResultantTargetHash sets ResultantTargetHash field to given value.

### HasResultantTargetHash

`func (o *MergeResponse) HasResultantTargetHash() bool`

HasResultantTargetHash returns a boolean if a field has been set.

### GetCommonAncestor

`func (o *MergeResponse) GetCommonAncestor() string`

GetCommonAncestor returns the CommonAncestor field if non-nil, zero value otherwise.

### GetCommonAncestorOk

`func (o *MergeResponse) GetCommonAncestorOk() (*string, bool)`

GetCommonAncestorOk returns a tuple with the CommonAncestor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommonAncestor

`func (o *MergeResponse) SetCommonAncestor(v string)`

SetCommonAncestor sets CommonAncestor field to given value.

### HasCommonAncestor

`func (o *MergeResponse) HasCommonAncestor() bool`

HasCommonAncestor returns a boolean if a field has been set.

### GetTargetBranch

`func (o *MergeResponse) GetTargetBranch() string`

GetTargetBranch returns the TargetBranch field if non-nil, zero value otherwise.

### GetTargetBranchOk

`func (o *MergeResponse) GetTargetBranchOk() (*string, bool)`

GetTargetBranchOk returns a tuple with the TargetBranch field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTargetBranch

`func (o *MergeResponse) SetTargetBranch(v string)`

SetTargetBranch sets TargetBranch field to given value.

### HasTargetBranch

`func (o *MergeResponse) HasTargetBranch() bool`

HasTargetBranch returns a boolean if a field has been set.

### GetEffectiveTargetHash

`func (o *MergeResponse) GetEffectiveTargetHash() string`

GetEffectiveTargetHash returns the EffectiveTargetHash field if non-nil, zero value otherwise.

### GetEffectiveTargetHashOk

`func (o *MergeResponse) GetEffectiveTargetHashOk() (*string, bool)`

GetEffectiveTargetHashOk returns a tuple with the EffectiveTargetHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveTargetHash

`func (o *MergeResponse) SetEffectiveTargetHash(v string)`

SetEffectiveTargetHash sets EffectiveTargetHash field to given value.

### HasEffectiveTargetHash

`func (o *MergeResponse) HasEffectiveTargetHash() bool`

HasEffectiveTargetHash returns a boolean if a field has been set.

### GetExpectedHash

`func (o *MergeResponse) GetExpectedHash() string`

GetExpectedHash returns the ExpectedHash field if non-nil, zero value otherwise.

### GetExpectedHashOk

`func (o *MergeResponse) GetExpectedHashOk() (*string, bool)`

GetExpectedHashOk returns a tuple with the ExpectedHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpectedHash

`func (o *MergeResponse) SetExpectedHash(v string)`

SetExpectedHash sets ExpectedHash field to given value.

### HasExpectedHash

`func (o *MergeResponse) HasExpectedHash() bool`

HasExpectedHash returns a boolean if a field has been set.

### GetDetails

`func (o *MergeResponse) GetDetails() []ContentKeyDetails`

GetDetails returns the Details field if non-nil, zero value otherwise.

### GetDetailsOk

`func (o *MergeResponse) GetDetailsOk() (*[]ContentKeyDetails, bool)`

GetDetailsOk returns a tuple with the Details field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDetails

`func (o *MergeResponse) SetDetails(v []ContentKeyDetails)`

SetDetails sets Details field to given value.

### HasDetails

`func (o *MergeResponse) HasDetails() bool`

HasDetails returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


