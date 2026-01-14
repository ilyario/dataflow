# Merge1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Message** | Pointer to **string** | Optional commit message for this merge request  If not set, the server will generate a commit message automatically using metadata from the  merged commits. | [optional] 
**FromHash** | Pointer to **string** | The hash of the last commit to merge.  This commit must be present in the history on &#39;fromRefName&#39; before the first common parent with respect to the target branch. | [optional] 
**FromRefName** | **string** | The name of the reference that contains the &#39;source&#39; commits for the requested merge or transplant operation.  | 
**KeyMergeModes** | Pointer to [**[]MergeKeyBehavior**](MergeKeyBehavior.md) | Specific merge behaviour requests by content key.  The default is set by the &#x60;defaultKeyMergeMode&#x60; parameter.  | [optional] 
**DefaultKeyMergeMode** | Pointer to [**MergeBehavior**](MergeBehavior.md) | The default merge mode. If not set, &#x60;NORMAL&#x60; is assumed.  This settings applies to key thaWhen set to &#39;true&#39; instructs the server to validate the request         but to avoid committing any changes.t are not explicitly mentioned in the &#x60;keyMergeModes&#x60; property.  | [optional] 
**DryRun** | Pointer to **bool** | When set to &#39;true&#39; instructs the server to validate the request but to avoid committing any changes.  | [optional] 
**FetchAdditionalInfo** | Pointer to **bool** | Whether to provide optional response data.  | [optional] 
**ReturnConflictAsResult** | Pointer to **bool** | When set to &#39;true&#39; instructs the server to produce normal (non-error) responses in case a conflict is detected and report conflict details in the response payload. | [optional] 
**CommitMeta** | Pointer to [**CommitMeta**](CommitMeta.md) |  | [optional] 

## Methods

### NewMerge1

`func NewMerge1(fromRefName string, ) *Merge1`

NewMerge1 instantiates a new Merge1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewMerge1WithDefaults

`func NewMerge1WithDefaults() *Merge1`

NewMerge1WithDefaults instantiates a new Merge1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetMessage

`func (o *Merge1) GetMessage() string`

GetMessage returns the Message field if non-nil, zero value otherwise.

### GetMessageOk

`func (o *Merge1) GetMessageOk() (*string, bool)`

GetMessageOk returns a tuple with the Message field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMessage

`func (o *Merge1) SetMessage(v string)`

SetMessage sets Message field to given value.

### HasMessage

`func (o *Merge1) HasMessage() bool`

HasMessage returns a boolean if a field has been set.

### GetFromHash

`func (o *Merge1) GetFromHash() string`

GetFromHash returns the FromHash field if non-nil, zero value otherwise.

### GetFromHashOk

`func (o *Merge1) GetFromHashOk() (*string, bool)`

GetFromHashOk returns a tuple with the FromHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFromHash

`func (o *Merge1) SetFromHash(v string)`

SetFromHash sets FromHash field to given value.

### HasFromHash

`func (o *Merge1) HasFromHash() bool`

HasFromHash returns a boolean if a field has been set.

### GetFromRefName

`func (o *Merge1) GetFromRefName() string`

GetFromRefName returns the FromRefName field if non-nil, zero value otherwise.

### GetFromRefNameOk

`func (o *Merge1) GetFromRefNameOk() (*string, bool)`

GetFromRefNameOk returns a tuple with the FromRefName field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFromRefName

`func (o *Merge1) SetFromRefName(v string)`

SetFromRefName sets FromRefName field to given value.


### GetKeyMergeModes

`func (o *Merge1) GetKeyMergeModes() []MergeKeyBehavior`

GetKeyMergeModes returns the KeyMergeModes field if non-nil, zero value otherwise.

### GetKeyMergeModesOk

`func (o *Merge1) GetKeyMergeModesOk() (*[]MergeKeyBehavior, bool)`

GetKeyMergeModesOk returns a tuple with the KeyMergeModes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKeyMergeModes

`func (o *Merge1) SetKeyMergeModes(v []MergeKeyBehavior)`

SetKeyMergeModes sets KeyMergeModes field to given value.

### HasKeyMergeModes

`func (o *Merge1) HasKeyMergeModes() bool`

HasKeyMergeModes returns a boolean if a field has been set.

### GetDefaultKeyMergeMode

`func (o *Merge1) GetDefaultKeyMergeMode() MergeBehavior`

GetDefaultKeyMergeMode returns the DefaultKeyMergeMode field if non-nil, zero value otherwise.

### GetDefaultKeyMergeModeOk

`func (o *Merge1) GetDefaultKeyMergeModeOk() (*MergeBehavior, bool)`

GetDefaultKeyMergeModeOk returns a tuple with the DefaultKeyMergeMode field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDefaultKeyMergeMode

`func (o *Merge1) SetDefaultKeyMergeMode(v MergeBehavior)`

SetDefaultKeyMergeMode sets DefaultKeyMergeMode field to given value.

### HasDefaultKeyMergeMode

`func (o *Merge1) HasDefaultKeyMergeMode() bool`

HasDefaultKeyMergeMode returns a boolean if a field has been set.

### GetDryRun

`func (o *Merge1) GetDryRun() bool`

GetDryRun returns the DryRun field if non-nil, zero value otherwise.

### GetDryRunOk

`func (o *Merge1) GetDryRunOk() (*bool, bool)`

GetDryRunOk returns a tuple with the DryRun field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDryRun

`func (o *Merge1) SetDryRun(v bool)`

SetDryRun sets DryRun field to given value.

### HasDryRun

`func (o *Merge1) HasDryRun() bool`

HasDryRun returns a boolean if a field has been set.

### GetFetchAdditionalInfo

`func (o *Merge1) GetFetchAdditionalInfo() bool`

GetFetchAdditionalInfo returns the FetchAdditionalInfo field if non-nil, zero value otherwise.

### GetFetchAdditionalInfoOk

`func (o *Merge1) GetFetchAdditionalInfoOk() (*bool, bool)`

GetFetchAdditionalInfoOk returns a tuple with the FetchAdditionalInfo field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFetchAdditionalInfo

`func (o *Merge1) SetFetchAdditionalInfo(v bool)`

SetFetchAdditionalInfo sets FetchAdditionalInfo field to given value.

### HasFetchAdditionalInfo

`func (o *Merge1) HasFetchAdditionalInfo() bool`

HasFetchAdditionalInfo returns a boolean if a field has been set.

### GetReturnConflictAsResult

`func (o *Merge1) GetReturnConflictAsResult() bool`

GetReturnConflictAsResult returns the ReturnConflictAsResult field if non-nil, zero value otherwise.

### GetReturnConflictAsResultOk

`func (o *Merge1) GetReturnConflictAsResultOk() (*bool, bool)`

GetReturnConflictAsResultOk returns a tuple with the ReturnConflictAsResult field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReturnConflictAsResult

`func (o *Merge1) SetReturnConflictAsResult(v bool)`

SetReturnConflictAsResult sets ReturnConflictAsResult field to given value.

### HasReturnConflictAsResult

`func (o *Merge1) HasReturnConflictAsResult() bool`

HasReturnConflictAsResult returns a boolean if a field has been set.

### GetCommitMeta

`func (o *Merge1) GetCommitMeta() CommitMeta`

GetCommitMeta returns the CommitMeta field if non-nil, zero value otherwise.

### GetCommitMetaOk

`func (o *Merge1) GetCommitMetaOk() (*CommitMeta, bool)`

GetCommitMetaOk returns a tuple with the CommitMeta field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitMeta

`func (o *Merge1) SetCommitMeta(v CommitMeta)`

SetCommitMeta sets CommitMeta field to given value.

### HasCommitMeta

`func (o *Merge1) HasCommitMeta() bool`

HasCommitMeta returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


