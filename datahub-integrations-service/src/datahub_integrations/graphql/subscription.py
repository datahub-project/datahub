GET_SUBSCRIPTION_QUERY = """
    query getSubscription($input: GetSubscriptionInput!) {
        getSubscription(input: $input) {
            subscription {
                actorUrn
                subscriptionUrn
            }
        }
    }
"""

CREATE_SUBSCRIPTION = """
    mutation createSubscription($input: CreateSubscriptionInput!) {
        createSubscription(input: $input) {
            actorUrn
            subscriptionUrn
        }
    }
"""

DELETE_SUBSCRIPTION = """
    mutation deleteSubscription($input: DeleteSubscriptionInput!) {
        deleteSubscription(input: $input)
    }
"""
