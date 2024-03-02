namespace ServiceBusUtil;

internal class TestEvent
{
    public int Id {get;set;}
    public string Name { get;set;}
    public Guid UniqueId { get;set;} = Guid.NewGuid();
}