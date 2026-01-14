# ReferenceMetadataV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**NumCommitsAhead** | Pointer to **int32** |  | [optional] 
**NumCommitsBehind** | Pointer to **int32** |  | [optional] 
**CommitMetaOfHEAD** | Pointer to [**CommitMetaV1**](CommitMetaV1.md) |  | [optional] 
**CommonAncestorHash** | Pointer to **string** |  | [optional] 
**NumTotalCommits** | Pointer to **int64** |  | [optional] 

## Methods

### NewReferenceMetadataV1

`func NewReferenceMetadataV1() *ReferenceMetadataV1`

NewReferenceMetadataV1 instantiates a new ReferenceMetadataV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewReferenceMetadataV1WithDefaults

`func NewReferenceMetadataV1WithDefaults() *ReferenceMetadataV1`

NewReferenceMetadataV1WithDefaults instantiates a new ReferenceMetadataV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetNumCommitsAhead

`func (o *ReferenceMetadataV1) GetNumCommitsAhead() int32`

GetNumCommitsAhead returns the NumCommitsAhead field if non-nil, zero value otherwise.

### GetNumCommitsAheadOk

`func (o *ReferenceMetadataV1) GetNumCommitsAheadOk() (*int32, bool)`

GetNumCommitsAheadOk returns a tuple with the NumCommitsAhead field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNumCommitsAhead

`func (o *ReferenceMetadataV1) SetNumCommitsAhead(v int32)`

SetNumCommitsAhead sets NumCommitsAhead field to given value.

### HasNumCommitsAhead

`func (o *ReferenceMetadataV1) HasNumCommitsAhead() bool`

HasNumCommitsAhead returns a boolean if a field has been set.

### GetNumCommitsBehind

`func (o *ReferenceMetadataV1) GetNumCommitsBehind() int32`

GetNumCommitsBehind returns the NumCommitsBehind field if non-nil, zero value otherwise.

### GetNumCommitsBehindOk

`func (o *ReferenceMetadataV1) GetNumCommitsBehindOk() (*int32, bool)`

GetNumCommitsBehindOk returns a tuple with the NumCommitsBehind field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNumCommitsBehind

`func (o *ReferenceMetadataV1) SetNumCommitsBehind(v int32)`

SetNumCommitsBehind sets NumCommitsBehind field to given value.

### HasNumCommitsBehind

`func (o *ReferenceMetadataV1) HasNumCommitsBehind() bool`

HasNumCommitsBehind returns a boolean if a field has been set.

### GetCommitMetaOfHEAD

`func (o *ReferenceMetadataV1) GetCommitMetaOfHEAD() CommitMetaV1`

GetCommitMetaOfHEAD returns the CommitMetaOfHEAD field if non-nil, zero value otherwise.

### GetCommitMetaOfHEADOk

`func (o *ReferenceMetadataV1) GetCommitMetaOfHEADOk() (*CommitMetaV1, bool)`

GetCommitMetaOfHEADOk returns a tuple with the CommitMetaOfHEAD field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitMetaOfHEAD

`func (o *ReferenceMetadataV1) SetCommitMetaOfHEAD(v CommitMetaV1)`

SetCommitMetaOfHEAD sets CommitMetaOfHEAD field to given value.

### HasCommitMetaOfHEAD

`func (o *ReferenceMetadataV1) HasCommitMetaOfHEAD() bool`

HasCommitMetaOfHEAD returns a boolean if a field has been set.

### GetCommonAncestorHash

`func (o *ReferenceMetadataV1) GetCommonAncestorHash() string`

GetCommonAncestorHash returns the CommonAncestorHash field if non-nil, zero value otherwise.

### GetCommonAncestorHashOk

`func (o *ReferenceMetadataV1) GetCommonAncestorHashOk() (*string, bool)`

GetCommonAncestorHashOk returns a tuple with the CommonAncestorHash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommonAncestorHash

`func (o *ReferenceMetadataV1) SetCommonAncestorHash(v string)`

SetCommonAncestorHash sets CommonAncestorHash field to given value.

### HasCommonAncestorHash

`func (o *ReferenceMetadataV1) HasCommonAncestorHash() bool`

HasCommonAncestorHash returns a boolean if a field has been set.

### GetNumTotalCommits

`func (o *ReferenceMetadataV1) GetNumTotalCommits() int64`

GetNumTotalCommits returns the NumTotalCommits field if non-nil, zero value otherwise.

### GetNumTotalCommitsOk

`func (o *ReferenceMetadataV1) GetNumTotalCommitsOk() (*int64, bool)`

GetNumTotalCommitsOk returns a tuple with the NumTotalCommits field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNumTotalCommits

`func (o *ReferenceMetadataV1) SetNumTotalCommits(v int64)`

SetNumTotalCommits sets NumTotalCommits field to given value.

### HasNumTotalCommits

`func (o *ReferenceMetadataV1) HasNumTotalCommits() bool`

HasNumTotalCommits returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


