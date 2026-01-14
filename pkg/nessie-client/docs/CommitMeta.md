# CommitMeta

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Hash** | Pointer to **string** |  | [optional] 
**Committer** | Pointer to **string** |  | [optional] 
**Author** | Pointer to **string** |  | [optional] 
**Authors** | **[]string** |  | 
**SignedOffBy** | Pointer to **string** |  | [optional] 
**AllSignedOffBy** | **[]string** |  | 
**Message** | **string** |  | 
**CommitTime** | Pointer to **time.Time** |  | [optional] 
**AuthorTime** | Pointer to **time.Time** |  | [optional] 
**Properties** | **map[string]string** |  | 
**AllProperties** | **map[string][]string** |  | 
**ParentCommitHashes** | **[]string** |  | 

## Methods

### NewCommitMeta

`func NewCommitMeta(authors []string, allSignedOffBy []string, message string, properties map[string]string, allProperties map[string][]string, parentCommitHashes []string, ) *CommitMeta`

NewCommitMeta instantiates a new CommitMeta object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCommitMetaWithDefaults

`func NewCommitMetaWithDefaults() *CommitMeta`

NewCommitMetaWithDefaults instantiates a new CommitMeta object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHash

`func (o *CommitMeta) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *CommitMeta) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *CommitMeta) SetHash(v string)`

SetHash sets Hash field to given value.

### HasHash

`func (o *CommitMeta) HasHash() bool`

HasHash returns a boolean if a field has been set.

### GetCommitter

`func (o *CommitMeta) GetCommitter() string`

GetCommitter returns the Committer field if non-nil, zero value otherwise.

### GetCommitterOk

`func (o *CommitMeta) GetCommitterOk() (*string, bool)`

GetCommitterOk returns a tuple with the Committer field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitter

`func (o *CommitMeta) SetCommitter(v string)`

SetCommitter sets Committer field to given value.

### HasCommitter

`func (o *CommitMeta) HasCommitter() bool`

HasCommitter returns a boolean if a field has been set.

### GetAuthor

`func (o *CommitMeta) GetAuthor() string`

GetAuthor returns the Author field if non-nil, zero value otherwise.

### GetAuthorOk

`func (o *CommitMeta) GetAuthorOk() (*string, bool)`

GetAuthorOk returns a tuple with the Author field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAuthor

`func (o *CommitMeta) SetAuthor(v string)`

SetAuthor sets Author field to given value.

### HasAuthor

`func (o *CommitMeta) HasAuthor() bool`

HasAuthor returns a boolean if a field has been set.

### GetAuthors

`func (o *CommitMeta) GetAuthors() []string`

GetAuthors returns the Authors field if non-nil, zero value otherwise.

### GetAuthorsOk

`func (o *CommitMeta) GetAuthorsOk() (*[]string, bool)`

GetAuthorsOk returns a tuple with the Authors field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAuthors

`func (o *CommitMeta) SetAuthors(v []string)`

SetAuthors sets Authors field to given value.


### GetSignedOffBy

`func (o *CommitMeta) GetSignedOffBy() string`

GetSignedOffBy returns the SignedOffBy field if non-nil, zero value otherwise.

### GetSignedOffByOk

`func (o *CommitMeta) GetSignedOffByOk() (*string, bool)`

GetSignedOffByOk returns a tuple with the SignedOffBy field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSignedOffBy

`func (o *CommitMeta) SetSignedOffBy(v string)`

SetSignedOffBy sets SignedOffBy field to given value.

### HasSignedOffBy

`func (o *CommitMeta) HasSignedOffBy() bool`

HasSignedOffBy returns a boolean if a field has been set.

### GetAllSignedOffBy

`func (o *CommitMeta) GetAllSignedOffBy() []string`

GetAllSignedOffBy returns the AllSignedOffBy field if non-nil, zero value otherwise.

### GetAllSignedOffByOk

`func (o *CommitMeta) GetAllSignedOffByOk() (*[]string, bool)`

GetAllSignedOffByOk returns a tuple with the AllSignedOffBy field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAllSignedOffBy

`func (o *CommitMeta) SetAllSignedOffBy(v []string)`

SetAllSignedOffBy sets AllSignedOffBy field to given value.


### GetMessage

`func (o *CommitMeta) GetMessage() string`

GetMessage returns the Message field if non-nil, zero value otherwise.

### GetMessageOk

`func (o *CommitMeta) GetMessageOk() (*string, bool)`

GetMessageOk returns a tuple with the Message field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMessage

`func (o *CommitMeta) SetMessage(v string)`

SetMessage sets Message field to given value.


### GetCommitTime

`func (o *CommitMeta) GetCommitTime() time.Time`

GetCommitTime returns the CommitTime field if non-nil, zero value otherwise.

### GetCommitTimeOk

`func (o *CommitMeta) GetCommitTimeOk() (*time.Time, bool)`

GetCommitTimeOk returns a tuple with the CommitTime field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitTime

`func (o *CommitMeta) SetCommitTime(v time.Time)`

SetCommitTime sets CommitTime field to given value.

### HasCommitTime

`func (o *CommitMeta) HasCommitTime() bool`

HasCommitTime returns a boolean if a field has been set.

### GetAuthorTime

`func (o *CommitMeta) GetAuthorTime() time.Time`

GetAuthorTime returns the AuthorTime field if non-nil, zero value otherwise.

### GetAuthorTimeOk

`func (o *CommitMeta) GetAuthorTimeOk() (*time.Time, bool)`

GetAuthorTimeOk returns a tuple with the AuthorTime field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAuthorTime

`func (o *CommitMeta) SetAuthorTime(v time.Time)`

SetAuthorTime sets AuthorTime field to given value.

### HasAuthorTime

`func (o *CommitMeta) HasAuthorTime() bool`

HasAuthorTime returns a boolean if a field has been set.

### GetProperties

`func (o *CommitMeta) GetProperties() map[string]string`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *CommitMeta) GetPropertiesOk() (*map[string]string, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *CommitMeta) SetProperties(v map[string]string)`

SetProperties sets Properties field to given value.


### GetAllProperties

`func (o *CommitMeta) GetAllProperties() map[string][]string`

GetAllProperties returns the AllProperties field if non-nil, zero value otherwise.

### GetAllPropertiesOk

`func (o *CommitMeta) GetAllPropertiesOk() (*map[string][]string, bool)`

GetAllPropertiesOk returns a tuple with the AllProperties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAllProperties

`func (o *CommitMeta) SetAllProperties(v map[string][]string)`

SetAllProperties sets AllProperties field to given value.


### GetParentCommitHashes

`func (o *CommitMeta) GetParentCommitHashes() []string`

GetParentCommitHashes returns the ParentCommitHashes field if non-nil, zero value otherwise.

### GetParentCommitHashesOk

`func (o *CommitMeta) GetParentCommitHashesOk() (*[]string, bool)`

GetParentCommitHashesOk returns a tuple with the ParentCommitHashes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetParentCommitHashes

`func (o *CommitMeta) SetParentCommitHashes(v []string)`

SetParentCommitHashes sets ParentCommitHashes field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


