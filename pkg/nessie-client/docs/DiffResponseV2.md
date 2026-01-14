# DiffResponseV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**HasMore** | Pointer to **bool** |  | [optional] 
**Token** | Pointer to **string** |  | [optional] 
**Diffs** | Pointer to [**[]DiffEntryV2**](DiffEntryV2.md) |  | [optional] 
**EffectiveFromReference** | Pointer to [**ReferenceV2**](ReferenceV2.md) |  | [optional] 
**EffectiveToReference** | Pointer to [**ReferenceV2**](ReferenceV2.md) |  | [optional] 

## Methods

### NewDiffResponseV2

`func NewDiffResponseV2() *DiffResponseV2`

NewDiffResponseV2 instantiates a new DiffResponseV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDiffResponseV2WithDefaults

`func NewDiffResponseV2WithDefaults() *DiffResponseV2`

NewDiffResponseV2WithDefaults instantiates a new DiffResponseV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHasMore

`func (o *DiffResponseV2) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *DiffResponseV2) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *DiffResponseV2) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *DiffResponseV2) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetToken

`func (o *DiffResponseV2) GetToken() string`

GetToken returns the Token field if non-nil, zero value otherwise.

### GetTokenOk

`func (o *DiffResponseV2) GetTokenOk() (*string, bool)`

GetTokenOk returns a tuple with the Token field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetToken

`func (o *DiffResponseV2) SetToken(v string)`

SetToken sets Token field to given value.

### HasToken

`func (o *DiffResponseV2) HasToken() bool`

HasToken returns a boolean if a field has been set.

### GetDiffs

`func (o *DiffResponseV2) GetDiffs() []DiffEntryV2`

GetDiffs returns the Diffs field if non-nil, zero value otherwise.

### GetDiffsOk

`func (o *DiffResponseV2) GetDiffsOk() (*[]DiffEntryV2, bool)`

GetDiffsOk returns a tuple with the Diffs field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDiffs

`func (o *DiffResponseV2) SetDiffs(v []DiffEntryV2)`

SetDiffs sets Diffs field to given value.

### HasDiffs

`func (o *DiffResponseV2) HasDiffs() bool`

HasDiffs returns a boolean if a field has been set.

### GetEffectiveFromReference

`func (o *DiffResponseV2) GetEffectiveFromReference() ReferenceV2`

GetEffectiveFromReference returns the EffectiveFromReference field if non-nil, zero value otherwise.

### GetEffectiveFromReferenceOk

`func (o *DiffResponseV2) GetEffectiveFromReferenceOk() (*ReferenceV2, bool)`

GetEffectiveFromReferenceOk returns a tuple with the EffectiveFromReference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveFromReference

`func (o *DiffResponseV2) SetEffectiveFromReference(v ReferenceV2)`

SetEffectiveFromReference sets EffectiveFromReference field to given value.

### HasEffectiveFromReference

`func (o *DiffResponseV2) HasEffectiveFromReference() bool`

HasEffectiveFromReference returns a boolean if a field has been set.

### GetEffectiveToReference

`func (o *DiffResponseV2) GetEffectiveToReference() ReferenceV2`

GetEffectiveToReference returns the EffectiveToReference field if non-nil, zero value otherwise.

### GetEffectiveToReferenceOk

`func (o *DiffResponseV2) GetEffectiveToReferenceOk() (*ReferenceV2, bool)`

GetEffectiveToReferenceOk returns a tuple with the EffectiveToReference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEffectiveToReference

`func (o *DiffResponseV2) SetEffectiveToReference(v ReferenceV2)`

SetEffectiveToReference sets EffectiveToReference field to given value.

### HasEffectiveToReference

`func (o *DiffResponseV2) HasEffectiveToReference() bool`

HasEffectiveToReference returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


