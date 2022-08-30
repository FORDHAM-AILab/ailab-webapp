import torch


def pso(func, lb, ub, args=(), kwargs={},
        particle_num=100, maxit=100, c1=0.5, c2=0.5, w=0.5,
        constraints_particle_adjust_func=None,
        device='cpu', verbose=False):
    lb = torch.tensor(lb, device=device)
    ub = torch.tensor(ub, device=device)
    dimension = len(lb)

    X = lb + (ub - lb) * torch.rand(particle_num, dimension, device=device)
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
            X = constraints_particle_adjust_func(X, *args, **kwargs)

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
