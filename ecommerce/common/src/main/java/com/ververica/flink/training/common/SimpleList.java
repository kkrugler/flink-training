package com.ververica.flink.training.common;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * A very simple implementation of something like ArrayList, but without the
 * problems caused by Kryo trying to change the visibility of internal
 * fields of java.* classes using the reflexion API.
 *
 * @param <E> the type of elements in this list
 */
public class SimpleList<E> implements Iterable<E> {

    private Object[] items;
    private int size;

    public SimpleList() {
        items = new Object[10];
        size = 0;
    }

    public SimpleList(SimpleList clone) {
        items = Arrays.copyOf(clone.items, clone.size);
        size = clone.size;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int newSize) {
        if (newSize > items.length) {
            items = Arrays.copyOf(items, newSize);
        }

        size = newSize;
    }

    public Object[] getItems() {
        return items;
    }

    public void setItems(Object[] items) {
        this.items = items;
        if (size > items.length) {
            size = items.length;
        }
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public E get(int index) {
        Objects.checkIndex(index, size);
        return (E)items[index];
    }

    public E set(int index, E record) {
        Objects.checkIndex(index, size);
        items[index] = record;
        return record;
    }

    public boolean add(E record) {
        if (size == items.length) {
            items = Arrays.copyOf(items, size + 100);
        }

        items[size] = record;
        size += 1;
        return true;
    }

    public E remove(int index) {
        @SuppressWarnings("unchecked") E result = (E) items[index];
        final int newSize = size - 1;
        System.arraycopy(items, index + 1, items, index, newSize - index);
        size = newSize;
        items[size] = null;

        return result;
    }


    @Override
    public Iterator<E> iterator() {

        return new Iterator<E>() {
            int cursor = 0;
            @Override
            public boolean hasNext() {
                return cursor < size;
            }

            @Override
            public E next() {
                return (E)items[cursor++];
            }
        };
    }
}
