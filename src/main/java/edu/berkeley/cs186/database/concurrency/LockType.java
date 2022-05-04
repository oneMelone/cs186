package edu.berkeley.cs186.database.concurrency;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    private static Map<LockType, Integer> getCommonLockTypeToIndex() {
        Map<LockType, Integer> lockTypeToIndex = new HashMap<>();
        lockTypeToIndex.put(NL, 0);
        lockTypeToIndex.put(IS, 1);
        lockTypeToIndex.put(IX, 2);
        lockTypeToIndex.put(S, 3);
        lockTypeToIndex.put(SIX, 4);
        lockTypeToIndex.put(X, 5);

        return lockTypeToIndex;
    }

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        // this is a matrix about compatibility, while the row and the column indices
        //  are bound to different types, as can be got in map lockTypeToIndex.
        ArrayList<List<Boolean>> compatibilityMatrix = new ArrayList<>();
        compatibilityMatrix.add(Arrays.asList(true, true, true, true, true, true));
        compatibilityMatrix.add(Arrays.asList(true, true, true, true, true, false));
        compatibilityMatrix.add(Arrays.asList(true, true, true, false, false, false));
        compatibilityMatrix.add(Arrays.asList(true, true, false, true, false, false));
        compatibilityMatrix.add(Arrays.asList(true, true, false, false, false, false));
        compatibilityMatrix.add(Arrays.asList(true, false, false, false, false, false));

        Map<LockType, Integer> lockTypeToIndex = getCommonLockTypeToIndex();

        return compatibilityMatrix.get(lockTypeToIndex.get(a)).get(lockTypeToIndex.get(b));
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // If the row index can be the parent of the column index, then true, otherwise false
        ArrayList<List<Boolean>> canBeParentMatrix = new ArrayList<>();
        canBeParentMatrix.add(Arrays.asList(true, false, false, false, false, false));
        canBeParentMatrix.add(Arrays.asList(true, true, false, true, false, false));
        canBeParentMatrix.add(Arrays.asList(true, true, true, true, true, true));
        List<Boolean> remainedRows = Arrays.asList(true, false, false, false, false, false);
        for (int i = 0; i < 3; i++) {
             canBeParentMatrix.add(remainedRows);
        }

        Map<LockType, Integer> lockTypeToIndex = getCommonLockTypeToIndex();

        return canBeParentMatrix.get(lockTypeToIndex.get(parentLockType)).get(lockTypeToIndex.get(childLockType));
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // If the row index can be substitute for the column index, then true
        ArrayList<List<Boolean>> substitutablityMatrix= new ArrayList<>();
        substitutablityMatrix.add(Arrays.asList(true, false, false, false, false, false));
        substitutablityMatrix.add(Arrays.asList(true, true, false, false, false, false));
        substitutablityMatrix.add(Arrays.asList(true, true, true, false, false, false));
        substitutablityMatrix.add(Arrays.asList(true, false, false, true, false, false));
        substitutablityMatrix.add(Arrays.asList(true, false, false, true, true, false));
        substitutablityMatrix.add(Arrays.asList(true, false, false, true, false, true));

        Map<LockType, Integer> lockTypeToIndex = getCommonLockTypeToIndex();

        return substitutablityMatrix.get(lockTypeToIndex.get(substitute)).get(lockTypeToIndex.get(required));
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

