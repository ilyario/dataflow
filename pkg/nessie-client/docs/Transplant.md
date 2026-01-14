# Transplant

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**FromRefName** | **string** |  | 
**HashesToTransplant** | **[]string** |  | 
**KeyMergeModes** | Pointer to [**[]MergeKeyBehavior**](MergeKeyBehavior.md) |  | [optional] 
**DefaultKeyMergeMode** | Pointer to [**MergeBehavior**](MergeBehavior.md) |  | [optional] 
**DryRun** | Pointer to **bool** |  | [optional] 
**FetchAdditionalInfo** | Pointer to **bool** |  | [optional] 
**ReturnConflictAsResult** | Pointer to **bool** |  | [optional] 

## Methods

### NewTransplant

`func NewTransplant(fromRefName string, hashesToTransplant []string, ) *Transplant`

NewTransplant instantiates a new Transplant object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTransplantWithDefaults

`func NewTransplantWithDefaults() *Transplant`

NewTransplantWithDefaults instantiates a new Transplant object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetFromRefName

`func (o *Transplant) GetFromRefName() string`

GetFromRefName returns the FromRefName field if non-nil, zero value otherwise.

### GetFromRefNameOk

`func (o *Transplant) GetFromRefNameOk() (*string, bool)`

GetFromRefNameOk returns a tuple with the FromRefName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFromRefName

`func (o *Transplant) SetFromRefName(v string)`

SetFromRefName sets FromRefName field to given value.


### GetHashesToTransplant

`func (o *Transplant) GetHashesToTransplant() []string`

GetHashesToTransplant returns the HashesToTransplant field if non-nil, zero value otherwise.

### GetHashesToTransplantOk

`func (o *Transplant) GetHashesToTransplantOk() (*[]string, bool)`

GetHashesToTransplantOk returns a tuple with the HashesToTransplant field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHashesToTransplant

`func (o *Transplant) SetHashesToTransplant(v []string)`

SetHashesToTransplant sets HashesToTransplant field to given value.


### GetKeyMergeModes

`func (o *Transplant) GetKeyMergeModes() []MergeKeyBehavior`

GetKeyMergeModes returns the KeyMergeModes field if non-nil, zero value otherwise.

### GetKeyMergeModesOk

`func (o *Transplant) GetKeyMergeModesOk() (*[]MergeKeyBehavior, bool)`

GetKeyMergeModesOk returns a tuple with the KeyMergeModes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKeyMergeModes

`func (o *Transplant) SetKeyMergeModes(v []MergeKeyBehavior)`

SetKeyMergeModes sets KeyMergeModes field to given value.

### HasKeyMergeModes

`func (o *Transplant) HasKeyMergeModes() bool`

HasKeyMergeModes returns a boolean if a field has been set.

### GetDefaultKeyMergeMode

`func (o *Transplant) GetDefaultKeyMergeMode() MergeBehavior`

GetDefaultKeyMergeMode returns the DefaultKeyMergeMode field if non-nil, zero value otherwise.

### GetDefaultKeyMergeModeOk

`func (o *Transplant) GetDefaultKeyMergeModeOk() (*MergeBehavior, bool)`

GetDefaultKeyMergeModeOk returns a tuple with the DefaultKeyMergeMode field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefaultKeyMergeMode

`func (o *Transplant) SetDefaultKeyMergeMode(v MergeBehavior)`

SetDefaultKeyMergeMode sets DefaultKeyMergeMode field to given value.

### HasDefaultKeyMergeMode

`func (o *Transplant) HasDefaultKeyMergeMode() bool`

HasDefaultKeyMergeMode returns a boolean if a field has been set.

### GetDryRun

`func (o *Transplant) GetDryRun() bool`

GetDryRun returns the DryRun field if non-nil, zero value otherwise.

### GetDryRunOk

`func (o *Transplant) GetDryRunOk() (*bool, bool)`

GetDryRunOk returns a tuple with the DryRun field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDryRun

`func (o *Transplant) SetDryRun(v bool)`

SetDryRun sets DryRun field to given value.

### HasDryRun

`func (o *Transplant) HasDryRun() bool`

HasDryRun returns a boolean if a field has been set.

### GetFetchAdditionalInfo

`func (o *Transplant) GetFetchAdditionalInfo() bool`

GetFetchAdditionalInfo returns the FetchAdditionalInfo field if non-nil, zero value otherwise.

### GetFetchAdditionalInfoOk

`func (o *Transplant) GetFetchAdditionalInfoOk() (*bool, bool)`

GetFetchAdditionalInfoOk returns a tuple with the FetchAdditionalInfo field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFetchAdditionalInfo

`func (o *Transplant) SetFetchAdditionalInfo(v bool)`

SetFetchAdditionalInfo sets FetchAdditionalInfo field to given value.

### HasFetchAdditionalInfo

`func (o *Transplant) HasFetchAdditionalInfo() bool`

HasFetchAdditionalInfo returns a boolean if a field has been set.

### GetReturnConflictAsResult

`func (o *Transplant) GetReturnConflictAsResult() bool`

GetReturnConflictAsResult returns the ReturnConflictAsResult field if non-nil, zero value otherwise.

### GetReturnConflictAsResultOk

`func (o *Transplant) GetReturnConflictAsResultOk() (*bool, bool)`

GetReturnConflictAsResultOk returns a tuple with the ReturnConflictAsResult field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReturnConflictAsResult

`func (o *Transplant) SetReturnConflictAsResult(v bool)`

SetReturnConflictAsResult sets ReturnConflictAsResult field to given value.

### HasReturnConflictAsResult

`func (o *Transplant) HasReturnConflictAsResult() bool`

HasReturnConflictAsResult returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


