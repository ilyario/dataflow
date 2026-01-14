# Merge

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**FromRefName** | **string** |  | 
**FromHash** | **string** |  | 
**KeyMergeModes** | Pointer to [**[]MergeKeyBehavior**](MergeKeyBehavior.md) |  | [optional] 
**DefaultKeyMergeMode** | Pointer to [**MergeBehavior**](MergeBehavior.md) |  | [optional] 
**DryRun** | Pointer to **bool** |  | [optional] 
**FetchAdditionalInfo** | Pointer to **bool** |  | [optional] 
**ReturnConflictAsResult** | Pointer to **bool** |  | [optional] 

## Methods

### NewMerge

`func NewMerge(fromRefName string, fromHash string, ) *Merge`

NewMerge instantiates a new Merge object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewMergeWithDefaults

`func NewMergeWithDefaults() *Merge`

NewMergeWithDefaults instantiates a new Merge object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetFromRefName

`func (o *Merge) GetFromRefName() string`

GetFromRefName returns the FromRefName field if non-nil, zero value otherwise.

### GetFromRefNameOk

`func (o *Merge) GetFromRefNameOk() (*string, bool)`

GetFromRefNameOk returns a tuple with the FromRefName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFromRefName

`func (o *Merge) SetFromRefName(v string)`

SetFromRefName sets FromRefName field to given value.


### GetFromHash

`func (o *Merge) GetFromHash() string`

GetFromHash returns the FromHash field if non-nil, zero value otherwise.

### GetFromHashOk

`func (o *Merge) GetFromHashOk() (*string, bool)`

GetFromHashOk returns a tuple with the FromHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFromHash

`func (o *Merge) SetFromHash(v string)`

SetFromHash sets FromHash field to given value.


### GetKeyMergeModes

`func (o *Merge) GetKeyMergeModes() []MergeKeyBehavior`

GetKeyMergeModes returns the KeyMergeModes field if non-nil, zero value otherwise.

### GetKeyMergeModesOk

`func (o *Merge) GetKeyMergeModesOk() (*[]MergeKeyBehavior, bool)`

GetKeyMergeModesOk returns a tuple with the KeyMergeModes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKeyMergeModes

`func (o *Merge) SetKeyMergeModes(v []MergeKeyBehavior)`

SetKeyMergeModes sets KeyMergeModes field to given value.

### HasKeyMergeModes

`func (o *Merge) HasKeyMergeModes() bool`

HasKeyMergeModes returns a boolean if a field has been set.

### GetDefaultKeyMergeMode

`func (o *Merge) GetDefaultKeyMergeMode() MergeBehavior`

GetDefaultKeyMergeMode returns the DefaultKeyMergeMode field if non-nil, zero value otherwise.

### GetDefaultKeyMergeModeOk

`func (o *Merge) GetDefaultKeyMergeModeOk() (*MergeBehavior, bool)`

GetDefaultKeyMergeModeOk returns a tuple with the DefaultKeyMergeMode field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefaultKeyMergeMode

`func (o *Merge) SetDefaultKeyMergeMode(v MergeBehavior)`

SetDefaultKeyMergeMode sets DefaultKeyMergeMode field to given value.

### HasDefaultKeyMergeMode

`func (o *Merge) HasDefaultKeyMergeMode() bool`

HasDefaultKeyMergeMode returns a boolean if a field has been set.

### GetDryRun

`func (o *Merge) GetDryRun() bool`

GetDryRun returns the DryRun field if non-nil, zero value otherwise.

### GetDryRunOk

`func (o *Merge) GetDryRunOk() (*bool, bool)`

GetDryRunOk returns a tuple with the DryRun field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDryRun

`func (o *Merge) SetDryRun(v bool)`

SetDryRun sets DryRun field to given value.

### HasDryRun

`func (o *Merge) HasDryRun() bool`

HasDryRun returns a boolean if a field has been set.

### GetFetchAdditionalInfo

`func (o *Merge) GetFetchAdditionalInfo() bool`

GetFetchAdditionalInfo returns the FetchAdditionalInfo field if non-nil, zero value otherwise.

### GetFetchAdditionalInfoOk

`func (o *Merge) GetFetchAdditionalInfoOk() (*bool, bool)`

GetFetchAdditionalInfoOk returns a tuple with the FetchAdditionalInfo field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFetchAdditionalInfo

`func (o *Merge) SetFetchAdditionalInfo(v bool)`

SetFetchAdditionalInfo sets FetchAdditionalInfo field to given value.

### HasFetchAdditionalInfo

`func (o *Merge) HasFetchAdditionalInfo() bool`

HasFetchAdditionalInfo returns a boolean if a field has been set.

### GetReturnConflictAsResult

`func (o *Merge) GetReturnConflictAsResult() bool`

GetReturnConflictAsResult returns the ReturnConflictAsResult field if non-nil, zero value otherwise.

### GetReturnConflictAsResultOk

`func (o *Merge) GetReturnConflictAsResultOk() (*bool, bool)`

GetReturnConflictAsResultOk returns a tuple with the ReturnConflictAsResult field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReturnConflictAsResult

`func (o *Merge) SetReturnConflictAsResult(v bool)`

SetReturnConflictAsResult sets ReturnConflictAsResult field to given value.

### HasReturnConflictAsResult

`func (o *Merge) HasReturnConflictAsResult() bool`

HasReturnConflictAsResult returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


