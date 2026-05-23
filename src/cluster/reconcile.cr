require "json"

require "../hetzner/instance/delete"
require "../kubernetes/util"
require "../util"
require "../util/shell"

module Cluster
  module Reconcile
    # Reconcile the set of static worker instances against the rendered config.
    # Drains and deletes any cluster-labeled instance that is no longer present
    # in worker_node_pools. Autoscaler-managed instances and masters are never
    # touched. Opt-in via settings.reconcile_node_pools (default false).
    protected def reconcile_static_pools!
      return unless settings.reconcile_node_pools

      expected = expected_static_pool_instance_names
      autoscaler_set = autoscaler_managed_instance_names
      cluster_set = cluster_labeled_instance_names

      extras = cluster_set - expected - autoscaler_set

      if extras.empty?
        puts
        puts "Reconcile: cluster state matches rendered config, nothing to remove.".colorize(:green)
        return
      end

      puts
      puts "Reconcile: removing #{extras.size} stale worker instance(s): #{extras.join(", ")}".colorize(:yellow)
      drain_and_delete_extras(extras)
    end

    private def expected_static_pool_instance_names : Array(String)
      names = [] of String

      masters_pool = settings.masters_pool
      masters_pool.instance_count.times do |i|
        names << instance_builder.build_instance_name(
          masters_pool.instance_type, i, settings.include_instance_type_in_instance_name, "master"
        )
        if legacy = masters_pool.legacy_instance_type
          names << instance_builder.build_instance_name(legacy, i, true, "master")
        end
      end

      static_pools = settings.worker_node_pools.reject(&.autoscaling_enabled)
      static_pools.each do |pool|
        pool.instance_count.times do |i|
          prefix = "pool-#{pool.name}-worker"
          names << instance_builder.build_instance_name(
            pool.instance_type, i, settings.include_instance_type_in_instance_name, prefix
          )
          if legacy = pool.legacy_instance_type
            names << instance_builder.build_instance_name(legacy, i, true, prefix)
          end
        end
      end

      names
    end

    private def autoscaler_managed_instance_names : Array(String)
      names = [] of String
      settings.worker_node_pools.each do |pool|
        next unless pool.autoscaling_enabled
        node_group_name = pool.include_cluster_name_as_prefix ? "#{settings.cluster_name}-#{pool.name}" : pool.name
        fetch_instance_names_by_label("hcloud/node-group=#{node_group_name}", names)
      end
      names
    end

    private def cluster_labeled_instance_names : Array(String)
      names = [] of String
      fetch_instance_names_by_label("cluster=#{settings.cluster_name}", names)
      names
    end

    private def fetch_instance_names_by_label(label_selector : String, instance_names : Array(String))
      success, response = hetzner_client.get("/servers", {:label_selector => label_selector})
      return unless success

      JSON.parse(response)["servers"].as_a.each do |instance_data|
        instance_name = instance_data["name"].as_s
        instance_names << instance_name unless instance_names.includes?(instance_name)
      end
    end

    private def drain_and_delete_extras(extras : Array(String))
      channel = Channel(String | Exception).new
      semaphore = Channel(Nil).new(5)

      extras.each do |instance_name|
        semaphore.send(nil)
        spawn do
          begin
            puts "Draining node #{instance_name}..."
            drain_result = drain_node(instance_name)

            unless drain_result.success?
              puts "Warning: drain failed for #{instance_name}, skipping delete. Re-run after resolving the eviction issue.".colorize(:yellow)
              channel.send(instance_name)
              next
            end

            delete_node_from_kubernetes(instance_name)

            puts "Deleting Hetzner instance #{instance_name}..."
            Hetzner::Instance::Delete.new(
              settings: settings,
              hetzner_client: hetzner_client,
              instance_name: instance_name
            ).run

            channel.send(instance_name)
          rescue e : Exception
            channel.send(e)
          ensure
            semaphore.receive
          end
        end
      end

      errors = [] of Exception
      extras.size.times do
        result = channel.receive
        errors << result if result.is_a?(Exception)
      end

      unless errors.empty?
        errors.each { |e| puts "Reconcile error: #{e.message}".colorize(:red) }
      end
    end
  end
end
