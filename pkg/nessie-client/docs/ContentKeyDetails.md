# ContentKeyDetails

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Key** | Pointer to [**ContentKey**](ContentKey.md) |  | [optional] 
**MergeBehavior** | Pointer to [**MergeBehavior**](MergeBehavior.md) |  | [optional] 
**Conflict** | Pointer to [**Conflict**](Conflict.md) |  | [optional] 

## Methods

### NewContentKeyDetails

`func NewContentKeyDetails() *ContentKeyDetails`

NewContentKeyDetails instantiates a new ContentKeyDetails object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewContentKeyDetailsWithDefaults

`func NewContentKeyDetailsWithDefaults() *ContentKeyDetails`

NewContentKeyDetailsWithDefaults instantiates a new ContentKeyDetails object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetKey

`func (o *ContentKeyDetails) GetKey() ContentKey`

GetKey returns the Key field if non-nil, zero value otherwise.

### GetKeyOk

`func (o *ContentKeyDetails) GetKeyOk() (*ContentKey, bool)`

GetKeyOk returns a tuple with the Key field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKey

`func (o *ContentKeyDetails) SetKey(v ContentKey)`

SetKey sets Key field to given value.

### HasKey

`func (o *ContentKeyDetails) HasKey() bool`

HasKey returns a boolean if a field has been set.

### GetMergeBehavior

`func (o *ContentKeyDetails) GetMergeBehavior() MergeBehavior`

GetMergeBehavior returns the MergeBehavior field if non-nil, zero value otherwise.

### GetMergeBehaviorOk

`func (o *ContentKeyDetails) GetMergeBehaviorOk() (*MergeBehavior, bool)`

GetMergeBehaviorOk returns a tuple with the MergeBehavior field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMergeBehavior

`func (o *ContentKeyDetails) SetMergeBehavior(v MergeBehavior)`

SetMergeBehavior sets MergeBehavior field to given value.

### HasMergeBehavior

`func (o *ContentKeyDetails) HasMergeBehavior() bool`

HasMergeBehavior returns a boolean if a field has been set.

### GetConflict

`func (o *ContentKeyDetails) GetConflict() Conflict`

GetConflict returns the Conflict field if non-nil, zero value otherwise.

### GetConflictOk

`func (o *ContentKeyDetails) GetConflictOk() (*Conflict, bool)`

GetConflictOk returns a tuple with the Conflict field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConflict

`func (o *ContentKeyDetails) SetConflict(v Conflict)`

SetConflict sets Conflict field to given value.

### HasConflict

`func (o *ContentKeyDetails) HasConflict() bool`

HasConflict returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


