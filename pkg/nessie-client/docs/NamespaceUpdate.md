# NamespaceUpdate

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PropertyUpdates** | Pointer to **map[string]string** |  | [optional] 
**PropertyRemovals** | Pointer to **[]string** |  | [optional] 

## Methods

### NewNamespaceUpdate

`func NewNamespaceUpdate() *NamespaceUpdate`

NewNamespaceUpdate instantiates a new NamespaceUpdate object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewNamespaceUpdateWithDefaults

`func NewNamespaceUpdateWithDefaults() *NamespaceUpdate`

NewNamespaceUpdateWithDefaults instantiates a new NamespaceUpdate object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPropertyUpdates

`func (o *NamespaceUpdate) GetPropertyUpdates() map[string]string`

GetPropertyUpdates returns the PropertyUpdates field if non-nil, zero value otherwise.

### GetPropertyUpdatesOk

`func (o *NamespaceUpdate) GetPropertyUpdatesOk() (*map[string]string, bool)`

GetPropertyUpdatesOk returns a tuple with the PropertyUpdates field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPropertyUpdates

`func (o *NamespaceUpdate) SetPropertyUpdates(v map[string]string)`

SetPropertyUpdates sets PropertyUpdates field to given value.

### HasPropertyUpdates

`func (o *NamespaceUpdate) HasPropertyUpdates() bool`

HasPropertyUpdates returns a boolean if a field has been set.

### GetPropertyRemovals

`func (o *NamespaceUpdate) GetPropertyRemovals() []string`

GetPropertyRemovals returns the PropertyRemovals field if non-nil, zero value otherwise.

### GetPropertyRemovalsOk

`func (o *NamespaceUpdate) GetPropertyRemovalsOk() (*[]string, bool)`

GetPropertyRemovalsOk returns a tuple with the PropertyRemovals field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPropertyRemovals

`func (o *NamespaceUpdate) SetPropertyRemovals(v []string)`

SetPropertyRemovals sets PropertyRemovals field to given value.

### HasPropertyRemovals

`func (o *NamespaceUpdate) HasPropertyRemovals() bool`

HasPropertyRemovals returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


