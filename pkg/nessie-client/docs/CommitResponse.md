# CommitResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**TargetBranch** | [**Branch**](Branch.md) |  | 
**AddedContents** | Pointer to [**[]AddedContent**](AddedContent.md) |  | [optional] 

## Methods

### NewCommitResponse

`func NewCommitResponse(targetBranch Branch, ) *CommitResponse`

NewCommitResponse instantiates a new CommitResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCommitResponseWithDefaults

`func NewCommitResponseWithDefaults() *CommitResponse`

NewCommitResponseWithDefaults instantiates a new CommitResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetTargetBranch

`func (o *CommitResponse) GetTargetBranch() Branch`

GetTargetBranch returns the TargetBranch field if non-nil, zero value otherwise.

### GetTargetBranchOk

`func (o *CommitResponse) GetTargetBranchOk() (*Branch, bool)`

GetTargetBranchOk returns a tuple with the TargetBranch field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTargetBranch

`func (o *CommitResponse) SetTargetBranch(v Branch)`

SetTargetBranch sets TargetBranch field to given value.


### GetAddedContents

`func (o *CommitResponse) GetAddedContents() []AddedContent`

GetAddedContents returns the AddedContents field if non-nil, zero value otherwise.

### GetAddedContentsOk

`func (o *CommitResponse) GetAddedContentsOk() (*[]AddedContent, bool)`

GetAddedContentsOk returns a tuple with the AddedContents field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAddedContents

`func (o *CommitResponse) SetAddedContents(v []AddedContent)`

SetAddedContents sets AddedContents field to given value.

### HasAddedContents

`func (o *CommitResponse) HasAddedContents() bool`

HasAddedContents returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


