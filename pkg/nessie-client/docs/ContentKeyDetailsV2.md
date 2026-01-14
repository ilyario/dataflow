# ContentKeyDetailsV2

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Key** | Pointer to [**ContentKeyV2**](ContentKeyV2.md) |  | [optional] 
**MergeBehavior** | Pointer to [**MergeBehaviorV2**](MergeBehaviorV2.md) |  | [optional] 
**Conflict** | Pointer to [**ConflictV2**](ConflictV2.md) |  | [optional] 

## Methods

### NewContentKeyDetailsV2

`func NewContentKeyDetailsV2() *ContentKeyDetailsV2`

NewContentKeyDetailsV2 instantiates a new ContentKeyDetailsV2 object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewContentKeyDetailsV2WithDefaults

`func NewContentKeyDetailsV2WithDefaults() *ContentKeyDetailsV2`

NewContentKeyDetailsV2WithDefaults instantiates a new ContentKeyDetailsV2 object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetKey

`func (o *ContentKeyDetailsV2) GetKey() ContentKeyV2`

GetKey returns the Key field if non-nil, zero value otherwise.

### GetKeyOk

`func (o *ContentKeyDetailsV2) GetKeyOk() (*ContentKeyV2, bool)`

GetKeyOk returns a tuple with the Key field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetKey

`func (o *ContentKeyDetailsV2) SetKey(v ContentKeyV2)`

SetKey sets Key field to given value.

### HasKey

`func (o *ContentKeyDetailsV2) HasKey() bool`

HasKey returns a boolean if a field has been set.

### GetMergeBehavior

`func (o *ContentKeyDetailsV2) GetMergeBehavior() MergeBehaviorV2`

GetMergeBehavior returns the MergeBehavior field if non-nil, zero value otherwise.

### GetMergeBehaviorOk

`func (o *ContentKeyDetailsV2) GetMergeBehaviorOk() (*MergeBehaviorV2, bool)`

GetMergeBehaviorOk returns a tuple with the MergeBehavior field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMergeBehavior

`func (o *ContentKeyDetailsV2) SetMergeBehavior(v MergeBehaviorV2)`

SetMergeBehavior sets MergeBehavior field to given value.

### HasMergeBehavior

`func (o *ContentKeyDetailsV2) HasMergeBehavior() bool`

HasMergeBehavior returns a boolean if a field has been set.

### GetConflict

`func (o *ContentKeyDetailsV2) GetConflict() ConflictV2`

GetConflict returns the Conflict field if non-nil, zero value otherwise.

### GetConflictOk

`func (o *ContentKeyDetailsV2) GetConflictOk() (*ConflictV2, bool)`

GetConflictOk returns a tuple with the Conflict field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConflict

`func (o *ContentKeyDetailsV2) SetConflict(v ConflictV2)`

SetConflict sets Conflict field to given value.

### HasConflict

`func (o *ContentKeyDetailsV2) HasConflict() bool`

HasConflict returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


