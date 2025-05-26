# Lab: Bootcamp Review (Discussion)

We'll work through the solution to the exercise.

## Exercise 1 Solution

See the [README](README.md#exercise-1) file for the steps.

1. Filter out uncompleted transactions and foreign (not US)
   ```java
      .filter(new RemoveUncompletedAndNotUSFilter());

   public class RemoveUncompletedAndNotUSFilter implements FilterFunction<ShoppingCartRecord> {
      public boolean filter(ShoppingCartRecord in) throws Exception {
         return in.isTransactionCompleted() && (in.getCountry().equals("US"));
      }
   }
   ```
   Normally for a simple filter you'd use a lambda, like:

   ```java
      .filter(r -> r.isTransactionCompleted());
    ```

   But as the logic gets more complicated, using a separate function makes it easier to
   validate via Flink's test harness.
1. Calculate total cost
   ```java
      .map(new CalcTotalCostMap())

   public class CalcTotalCostMap implements MapFunction<ShoppingCartRecord, ShoppingCartWithCost> {
      public ShoppingCartWithCost map(ShoppingCartRecord value) throws Exception {
         double totalCost = 0;
         for (CartItem item : value.getItems()) {
            totalCost += (item.getPrice() * item.getQuantity());
         }

         ShoppingCartWithCost result = new ShoppingCartWithCost(value);
         result.setCost(totalCost);
         return result;
      }
   }
   ```
1. Explode a shopping cart record into individual cart items, with some shopping cart info.
   ```java
      .flatMap(new ExplodeCartItemsFlatMap())

   public class ExplodeCartItemsFlatMap implements FlatMapFunction<ShoppingCartWithCost, CartItemWithShoppingCartInfo> {
      public void flatMap(ShoppingCartWithCost in, Collector<CartItemWithShoppingCartInfo> out) throws Exception {
         for (CartItem item : in.getItems()) {
            out.collect(new CartItemWithShoppingCartInfo(item, in.getTransactionId(), in.getCost()));
         }
      }
   }
   ```

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
