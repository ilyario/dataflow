# ReferenceHistoryResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Reference** | [**Reference**](Reference.md) |  | 
**Current** | [**ReferenceHistoryState**](ReferenceHistoryState.md) | Consistency status of the current HEAD commit. | 
**Previous** | [**[]ReferenceHistoryState**](ReferenceHistoryState.md) | Consistency status of the recorded recent HEADs of the reference, including re-assign operations. | 
**CommitLogConsistency** | [**CommitConsistency**](CommitConsistency.md) | Combined consistency status of the commit-log of the reference, if requested by the client. | 

## Methods

### NewReferenceHistoryResponse

`func NewReferenceHistoryResponse(reference Reference, current ReferenceHistoryState, previous []ReferenceHistoryState, commitLogConsistency CommitConsistency, ) *ReferenceHistoryResponse`

NewReferenceHistoryResponse instantiates a new ReferenceHistoryResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceHistoryResponseWithDefaults

`func NewReferenceHistoryResponseWithDefaults() *ReferenceHistoryResponse`

NewReferenceHistoryResponseWithDefaults instantiates a new ReferenceHistoryResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetReference

`func (o *ReferenceHistoryResponse) GetReference() Reference`

GetReference returns the Reference field if non-nil, zero value otherwise.

### GetReferenceOk

`func (o *ReferenceHistoryResponse) GetReferenceOk() (*Reference, bool)`

GetReferenceOk returns a tuple with the Reference field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetReference

`func (o *ReferenceHistoryResponse) SetReference(v Reference)`

SetReference sets Reference field to given value.


### GetCurrent

`func (o *ReferenceHistoryResponse) GetCurrent() ReferenceHistoryState`

GetCurrent returns the Current field if non-nil, zero value otherwise.

### GetCurrentOk

`func (o *ReferenceHistoryResponse) GetCurrentOk() (*ReferenceHistoryState, bool)`

GetCurrentOk returns a tuple with the Current field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCurrent

`func (o *ReferenceHistoryResponse) SetCurrent(v ReferenceHistoryState)`

SetCurrent sets Current field to given value.


### GetPrevious

`func (o *ReferenceHistoryResponse) GetPrevious() []ReferenceHistoryState`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *ReferenceHistoryResponse) GetPreviousOk() (*[]ReferenceHistoryState, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *ReferenceHistoryResponse) SetPrevious(v []ReferenceHistoryState)`

SetPrevious sets Previous field to given value.


### GetCommitLogConsistency

`func (o *ReferenceHistoryResponse) GetCommitLogConsistency() CommitConsistency`

GetCommitLogConsistency returns the CommitLogConsistency field if non-nil, zero value otherwise.

### GetCommitLogConsistencyOk

`func (o *ReferenceHistoryResponse) GetCommitLogConsistencyOk() (*CommitConsistency, bool)`

GetCommitLogConsistencyOk returns a tuple with the CommitLogConsistency field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitLogConsistency

`func (o *ReferenceHistoryResponse) SetCommitLogConsistency(v CommitConsistency)`

SetCommitLogConsistency sets CommitLogConsistency field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


