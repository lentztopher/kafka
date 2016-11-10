package kafka.server

class NoOpLeaderElector extends LeaderElector {
  override def startup { }

  override def amILeader: Boolean = { return false }

  override def elect: Boolean = { return false }

  override def close { }

  override def resign { }
}
