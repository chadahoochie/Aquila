using System;
using System.Linq;
using System.Linq.Expressions;

namespace Aquila.Core.Queries;

public interface ICompiledQuery<TDoc, TResult> where TDoc : class
{
    Expression<Func<IQueryable<TDoc>, TResult>> QueryIs();
}
