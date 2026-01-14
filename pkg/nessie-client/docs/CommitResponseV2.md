# CommitResponseV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**TargetBranch** | [**BranchV2**](BranchV2.md) |  | 
**AddedContents** | Pointer to [**[]AddedContentV2**](AddedContentV2.md) |  | [optional] 

## Methods

### NewCommitResponseV2

`func NewCommitResponseV2(targetBranch BranchV2, ) *CommitResponseV2`

NewCommitResponseV2 instantiates a new CommitResponseV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCommitResponseV2WithDefaults

`func NewCommitResponseV2WithDefaults() *CommitResponseV2`

NewCommitResponseV2WithDefaults instantiates a new CommitResponseV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetTargetBranch

`func (o *CommitResponseV2) GetTargetBranch() BranchV2`

GetTargetBranch returns the TargetBranch field if non-nil, zero value otherwise.

### GetTargetBranchOk

`func (o *CommitResponseV2) GetTargetBranchOk() (*BranchV2, bool)`

GetTargetBranchOk returns a tuple with the TargetBranch field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTargetBranch

`func (o *CommitResponseV2) SetTargetBranch(v BranchV2)`

SetTargetBranch sets TargetBranch field to given value.


### GetAddedContents

`func (o *CommitResponseV2) GetAddedContents() []AddedContentV2`

GetAddedContents returns the AddedContents field if non-nil, zero value otherwise.

### GetAddedContentsOk

`func (o *CommitResponseV2) GetAddedContentsOk() (*[]AddedContentV2, bool)`

GetAddedContentsOk returns a tuple with the AddedContents field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAddedContents

`func (o *CommitResponseV2) SetAddedContents(v []AddedContentV2)`

SetAddedContents sets AddedContents field to given value.

### HasAddedContents

`func (o *CommitResponseV2) HasAddedContents() bool`

HasAddedContents returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


