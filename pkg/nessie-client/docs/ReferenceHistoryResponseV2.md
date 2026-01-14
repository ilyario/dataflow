# ReferenceHistoryResponseV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Reference** | [**ReferenceV2**](ReferenceV2.md) |  | 
**Current** | [**ReferenceHistoryStateV2**](ReferenceHistoryStateV2.md) | Consistency status of the current HEAD commit. | 
**Previous** | [**[]ReferenceHistoryStateV2**](ReferenceHistoryStateV2.md) | Consistency status of the recorded recent HEADs of the reference, including re-assign operations. | 
**CommitLogConsistency** | [**CommitConsistencyV2**](CommitConsistencyV2.md) | Combined consistency status of the commit-log of the reference, if requested by the client. | 

## Methods

### NewReferenceHistoryResponseV2

`func NewReferenceHistoryResponseV2(reference ReferenceV2, current ReferenceHistoryStateV2, previous []ReferenceHistoryStateV2, commitLogConsistency CommitConsistencyV2, ) *ReferenceHistoryResponseV2`

NewReferenceHistoryResponseV2 instantiates a new ReferenceHistoryResponseV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceHistoryResponseV2WithDefaults

`func NewReferenceHistoryResponseV2WithDefaults() *ReferenceHistoryResponseV2`

NewReferenceHistoryResponseV2WithDefaults instantiates a new ReferenceHistoryResponseV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetReference

`func (o *ReferenceHistoryResponseV2) GetReference() ReferenceV2`

GetReference returns the Reference field if non-nil, zero value otherwise.

### GetReferenceOk

`func (o *ReferenceHistoryResponseV2) GetReferenceOk() (*ReferenceV2, bool)`

GetReferenceOk returns a tuple with the Reference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReference

`func (o *ReferenceHistoryResponseV2) SetReference(v ReferenceV2)`

SetReference sets Reference field to given value.


### GetCurrent

`func (o *ReferenceHistoryResponseV2) GetCurrent() ReferenceHistoryStateV2`

GetCurrent returns the Current field if non-nil, zero value otherwise.

### GetCurrentOk

`func (o *ReferenceHistoryResponseV2) GetCurrentOk() (*ReferenceHistoryStateV2, bool)`

GetCurrentOk returns a tuple with the Current field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCurrent

`func (o *ReferenceHistoryResponseV2) SetCurrent(v ReferenceHistoryStateV2)`

SetCurrent sets Current field to given value.


### GetPrevious

`func (o *ReferenceHistoryResponseV2) GetPrevious() []ReferenceHistoryStateV2`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *ReferenceHistoryResponseV2) GetPreviousOk() (*[]ReferenceHistoryStateV2, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *ReferenceHistoryResponseV2) SetPrevious(v []ReferenceHistoryStateV2)`

SetPrevious sets Previous field to given value.


### GetCommitLogConsistency

`func (o *ReferenceHistoryResponseV2) GetCommitLogConsistency() CommitConsistencyV2`

GetCommitLogConsistency returns the CommitLogConsistency field if non-nil, zero value otherwise.

### GetCommitLogConsistencyOk

`func (o *ReferenceHistoryResponseV2) GetCommitLogConsistencyOk() (*CommitConsistencyV2, bool)`

GetCommitLogConsistencyOk returns a tuple with the CommitLogConsistency field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitLogConsistency

`func (o *ReferenceHistoryResponseV2) SetCommitLogConsistency(v CommitConsistencyV2)`

SetCommitLogConsistency sets CommitLogConsistency field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


