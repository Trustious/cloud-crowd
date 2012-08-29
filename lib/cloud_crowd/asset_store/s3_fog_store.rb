gem 'fog'

module CloudCrowd
  class AssetStore

    # The S3FogStore is an implementation of an AssetStore that uses the Fog gem
    # to store all resulting files in an S3 bucket
    module S3FogStore

      # Configure authentication and establish a connection to S3, first thing.
      def setup
        @use_auth   = CloudCrowd.config[:s3_authentication]
        @expires    = CloudCrowd.config[:s3_url_lifetime] || 3600
        bucket_name = CloudCrowd.config[:s3_bucket]
        key, secret = CloudCrowd.config[:aws_access_key], CloudCrowd.config[:aws_secret_key]
        valid_conf  = [bucket_name, key, secret].all? {|s| s.is_a? String }
        raise Error::MissingConfiguration, "An S3 account must be configured in 'config.yml' before 's3_fog' storage can be used" unless valid_conf
        @s3         = Fog::Storage.new(:provider => "AWS",
                                        :aws_access_key_id => key, :aws_secret_access_key => secret)
        @bucket     = @s3.directories.get(bucket_name)
        @bucket     = @s3.directories.create(:key => bucket_name, :public => false) unless @bucket
        raise "Could not find or create the S3 bucket #{bucket_name}" unless @bucket
      end

      # Save a finished file from local storage to S3. Save it publicly unless
      # we're configured to use S3 authentication. Authenticated links expire
      # after one day by default.
      def save(local_path, save_path)
        file = @bucket.files.create(:key => save_path,
                                    :body => File.open(local_path),
                                    :public => !@use_auth)
        file.save
        if @use_auth
          file.url(Time.now.to_i + @expires)
        else
          file.public_url
        end
      end

      # Remove all of a Job's resulting files from S3, both intermediate and finished.
      def cleanup(job)
        @bucket.files.each {|f| f.destroy if f.key =~ /^#{job.action}\/job_#{job.id}/}
      end

    end

  end
end
