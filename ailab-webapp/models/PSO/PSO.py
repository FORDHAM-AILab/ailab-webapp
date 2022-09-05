import torch


def pso(func, dimension, bounds=None, args=(), kwargs={}, initial_guess=None,
        particle_num=100, maxit=20, c1=0.5, c2=0.5, w=0.5,
        constraints_particle_adjust_func=None, cons_args=(), cons_kwargs={},
        device='cpu', verbose=False):

    if initial_guess is None:
        if bounds is None:
            X = torch.rand(particle_num, dimension, device=device)
        else:
            X = bounds[0] + (bounds[1] - bounds[0]) * torch.rand(particle_num, dimension, device=device)
    else:
        X = torch.tensor(initial_guess, device=device)
    V = torch.rand(particle_num, dimension, device=device)

    pbest = torch.clone(X)
    pbest_obj = torch.tensor([func(x, *args, **kwargs) for x in X], device=device)
    gbest = pbest[0]
    gbest_obj = torch.tensor(float('inf'), device=device)

    for it in range(maxit):
        r1 = torch.rand(1, device=device)
        r2 = torch.rand(1, device=device)
        V = w*V + c1*r1*(pbest-X) + c2*r2*(gbest-X)
        X += V

        if constraints_particle_adjust_func is not None:
            X = constraints_particle_adjust_func(X, *cons_args, **cons_kwargs)

        obj = torch.tensor([func(x, *args, **kwargs) for x in X], device=device)
        pbest[(pbest_obj >= obj)] = X[(pbest_obj >= obj)]
        pbest_obj = torch.minimum(pbest_obj, obj)
        gbest = pbest[torch.argmin(pbest_obj)]
        gbest_obj = torch.min(pbest_obj)

        if verbose:
            print(f'iteration: {it+1}')
            print(f'best particle:\n{gbest}')
            print(f'best objective value: {float(gbest_obj)}')

    return pbest, pbest_obj, gbest, gbest_obj


# constraints_particle_adjust_functions

# With lower or upper bounds
def bounded(X, lower_bound=None, upper_bound=None):
    if lower_bound is not None:
        X = torch.maximum(X, lower_bound)
    if upper_bound is not None:
        X = torch.minimum(X, upper_bound)
    return X


# Non-decreasing or non-increasing
def monotonic(X, if_mono_increasing: bool, left_step=True):
    for x in X:
        if if_mono_increasing:
            if left_step:
                for i in range(len(x) - 1):
                    if x[i+1] < x[i]:
                        x[i+1] = x[i]
            else:
                for i in range(1, len(x)):
                    if x[-i] < x[-(i+1)]:
                        x[-(i+1)] = x[-i]
        else:
            if left_step:
                for i in range(len(x) - 1):
                    if x[i + 1] > x[i]:
                        x[i+1] = x[i]
            else:
                for i in range(1, len(x)):
                    if x[-i] > x[-(i+1)]:
                        x[-(i + 1)] = x[-i]
    return X
