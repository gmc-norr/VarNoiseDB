import math
import numpy as np

def update_stats(mean, std_dev, n, new_val, operation='add'):
    if operation not in ['add', 'remove']:
        raise ValueError("Operation must be 'add' or 'remove'")
    
    if n == 0 and operation == 'add':
        return new_val, 0, 1
    elif n == 1 and operation == 'remove':
        return 0, 0, 0
    
    if operation == 'add':
        n += 1
        var = std_dev ** 2 * (n - 1)
        delta = new_val - mean
        mean += delta / n
        delta2 = new_val - mean
        var += delta * delta2
    elif operation == 'remove':
        var = std_dev ** 2 * n
        delta = new_val - mean
        mean -= delta / n
        delta2 = new_val - mean
        var -= delta * delta2
        n -= 1
    
    std_dev = math.sqrt(var / n) if n > 0 else 0
    return mean, std_dev, n

def update_multiple_stats(means, std_devs, ns, new_vals, operation='add'):
    if operation not in ['add', 'remove']:
        raise ValueError("Operation must be 'add' or 'remove'")
    
    means = np.array(means)
    std_devs = np.array(std_devs)
    ns = np.array(ns)
    new_vals = np.array(new_vals)
    
    if operation == 'add':
        mask = ns == 0
        means[mask] = new_vals[mask]
        std_devs[mask] = 0
        ns[mask] = 1
        
        ns[~mask] += 1
        vars = std_devs[~mask] ** 2 * (ns[~mask] - 1)
        deltas = new_vals[~mask] - means[~mask]
        means[~mask] += deltas / ns[~mask]
        delta2s = new_vals[~mask] - means[~mask]
        vars += deltas * delta2s
    elif operation == 'remove':
        mask = ns == 1
        means[mask] = 0
        std_devs[mask] = 0
        ns[mask] = 0
        
        ns[~mask] -= 1
        vars = std_devs[~mask] ** 2 * (ns[~mask] + 1)
        deltas = new_vals[~mask] - means[~mask]
        means[~mask] -= deltas / (ns[~mask] + 1)
        delta2s = new_vals[~mask] - means[~mask]
        vars -= deltas * delta2s
    
    std_devs = np.sqrt(vars / ns) if np.any(ns > 0) else np.zeros_like(std_devs)
    return means, std_devs, ns