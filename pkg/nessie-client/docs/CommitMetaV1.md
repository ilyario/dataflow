# CommitMetaV1

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Hash** | Pointer to **string** |  | [optional] 
**Committer** | Pointer to **string** |  | [optional] 
**Author** | Pointer to **string** |  | [optional] 
**SignedOffBy** | Pointer to **string** |  | [optional] 
**Message** | **string** |  | 
**CommitTime** | Pointer to **time.Time** |  | [optional] 
**AuthorTime** | Pointer to **time.Time** |  | [optional] 
**Properties** | **map[string]string** |  | 

## Methods

### NewCommitMetaV1

`func NewCommitMetaV1(message string, properties map[string]string, ) *CommitMetaV1`

NewCommitMetaV1 instantiates a new CommitMetaV1 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCommitMetaV1WithDefaults

`func NewCommitMetaV1WithDefaults() *CommitMetaV1`

NewCommitMetaV1WithDefaults instantiates a new CommitMetaV1 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetHash

`func (o *CommitMetaV1) GetHash() string`

GetHash returns the Hash field if non-nil, zero value otherwise.

### GetHashOk

`func (o *CommitMetaV1) GetHashOk() (*string, bool)`

GetHashOk returns a tuple with the Hash field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHash

`func (o *CommitMetaV1) SetHash(v string)`

SetHash sets Hash field to given value.

### HasHash

`func (o *CommitMetaV1) HasHash() bool`

HasHash returns a boolean if a field has been set.

### GetCommitter

`func (o *CommitMetaV1) GetCommitter() string`

GetCommitter returns the Committer field if non-nil, zero value otherwise.

### GetCommitterOk

`func (o *CommitMetaV1) GetCommitterOk() (*string, bool)`

GetCommitterOk returns a tuple with the Committer field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitter

`func (o *CommitMetaV1) SetCommitter(v string)`

SetCommitter sets Committer field to given value.

### HasCommitter

`func (o *CommitMetaV1) HasCommitter() bool`

HasCommitter returns a boolean if a field has been set.

### GetAuthor

`func (o *CommitMetaV1) GetAuthor() string`

GetAuthor returns the Author field if non-nil, zero value otherwise.

### GetAuthorOk

`func (o *CommitMetaV1) GetAuthorOk() (*string, bool)`

GetAuthorOk returns a tuple with the Author field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAuthor

`func (o *CommitMetaV1) SetAuthor(v string)`

SetAuthor sets Author field to given value.

### HasAuthor

`func (o *CommitMetaV1) HasAuthor() bool`

HasAuthor returns a boolean if a field has been set.

### GetSignedOffBy

`func (o *CommitMetaV1) GetSignedOffBy() string`

GetSignedOffBy returns the SignedOffBy field if non-nil, zero value otherwise.

### GetSignedOffByOk

`func (o *CommitMetaV1) GetSignedOffByOk() (*string, bool)`

GetSignedOffByOk returns a tuple with the SignedOffBy field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSignedOffBy

`func (o *CommitMetaV1) SetSignedOffBy(v string)`

SetSignedOffBy sets SignedOffBy field to given value.

### HasSignedOffBy

`func (o *CommitMetaV1) HasSignedOffBy() bool`

HasSignedOffBy returns a boolean if a field has been set.

### GetMessage

`func (o *CommitMetaV1) GetMessage() string`

GetMessage returns the Message field if non-nil, zero value otherwise.

### GetMessageOk

`func (o *CommitMetaV1) GetMessageOk() (*string, bool)`

GetMessageOk returns a tuple with the Message field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMessage

`func (o *CommitMetaV1) SetMessage(v string)`

SetMessage sets Message field to given value.


### GetCommitTime

`func (o *CommitMetaV1) GetCommitTime() time.Time`

GetCommitTime returns the CommitTime field if non-nil, zero value otherwise.

### GetCommitTimeOk

`func (o *CommitMetaV1) GetCommitTimeOk() (*time.Time, bool)`

GetCommitTimeOk returns a tuple with the CommitTime field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCommitTime

`func (o *CommitMetaV1) SetCommitTime(v time.Time)`

SetCommitTime sets CommitTime field to given value.

### HasCommitTime

`func (o *CommitMetaV1) HasCommitTime() bool`

HasCommitTime returns a boolean if a field has been set.

### GetAuthorTime

`func (o *CommitMetaV1) GetAuthorTime() time.Time`

GetAuthorTime returns the AuthorTime field if non-nil, zero value otherwise.

### GetAuthorTimeOk

`func (o *CommitMetaV1) GetAuthorTimeOk() (*time.Time, bool)`

GetAuthorTimeOk returns a tuple with the AuthorTime field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAuthorTime

`func (o *CommitMetaV1) SetAuthorTime(v time.Time)`

SetAuthorTime sets AuthorTime field to given value.

### HasAuthorTime

`func (o *CommitMetaV1) HasAuthorTime() bool`

HasAuthorTime returns a boolean if a field has been set.

### GetProperties

`func (o *CommitMetaV1) GetProperties() map[string]string`

GetProperties returns the Properties field if non-nil, zero value otherwise.

### GetPropertiesOk

`func (o *CommitMetaV1) GetPropertiesOk() (*map[string]string, bool)`

GetPropertiesOk returns a tuple with the Properties field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProperties

`func (o *CommitMetaV1) SetProperties(v map[string]string)`

SetProperties sets Properties field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


